"""
clang_parser.py

Parse C code into a map which maps from (struct name) to
(base-class- and record-type- expanded fields list).
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Mapping, Tuple

import clang.cindex

logger = logging.getLogger("tscout")

# Expected path of this file: "postgres/cmudb/tscout/"

# Path to the Postgres root.
POSTGRES_PATH = Path(__file__).parent.parent.parent
# Path to the Postgres files to parse.
POSTGRES_FILES = (f"{POSTGRES_PATH}/src/backend/executor/execMain.c",)
# The arguments that Clang uses to parse header files.
CLANG_ARGS = [
    "-std=c17",
    f"-I{POSTGRES_PATH}/src/include",
    "-I/usr/lib/gcc/x86_64-linux-gnu/9/include",
    "-I/usr/local/include",
    "-I/usr/include/x86_64-linux-gnu",
    "-I/usr/include",
]


def convert_define_to_arg(input_define):
    """
    Convert from a #define to a command line arg.

    Parameters
    ----------
    input_define : str
        String in the format of "#define variable value".

    Returns
    -------
    output_str : str
        String in the format of "-Dvariable=value".
    """
    var_and_value = input_define.rstrip()[len("#define ") :]
    separator = var_and_value.find(" ")
    var = var_and_value[:separator]
    value = var_and_value[separator + 1 :]
    return f"-D{var}={value}"


# Grab the results of ./configure to make sure that we're passing the same
# preprocessor #defines to libclang as when compiling Postgres.
# #defines can affect struct sizing depending on machine environment.
with open(f"{POSTGRES_PATH}/config.log", encoding="utf-8") as config_file:
    for config_line in config_file:
        if config_line.startswith("#define "):
            CLANG_ARGS.append(convert_define_to_arg(config_line))


@dataclass
class Field:
    """A field of a struct, as parsed by Clang."""

    name: str
    pg_type: str
    canonical_type_kind: clang.cindex.TypeKind
    alignment: int = None  # Non-None for the first field of a struct, using alignment value of the struct.
    field_size: int = None  # Size of the field
    is_padding: bool = False  # Whether this field is a generated padding field.


class ClangParser:
    """
    On init, ClangParser parses the PostgreSQL source code to construct a
    mapping from struct name to a list of the struct fields, where the fields
    list has both base classes expanded and record types expanded.

    Attributes
    ----------
    field_map : Mapping[str, List[Field]]
        Maps from (struct name) to a (base-class- and record-type-
        expanded list of all fields for the struct).
    """

    def __init__(self):
        indexes: List[clang.cindex.Index] = []
        translation_units: List[clang.cindex.TranslationUnit] = []
        classes: Mapping[str, clang.cindex.Cursor] = {}
        enums: Mapping[str, clang.cindex.Cursor] = {}

        # Parse each postgres file's definitions into the classes map.
        # classes is a map to handle potential duplicate definitions
        # from parsing multiple translation units.
        for postgres_file in POSTGRES_FILES:
            # Parse the translation unit.
            index = clang.cindex.Index.create()
            tunit = index.parse(postgres_file, args=CLANG_ARGS)

            # Keep the index and translation unit alive for the rest of init.
            indexes.append(index)
            translation_units.append(tunit)

            # Add all relevant definitions to the classes map.
            for node in tunit.cursor.get_children():
                kind_ok = node.kind in [
                    clang.cindex.CursorKind.CLASS_DECL,
                    clang.cindex.CursorKind.STRUCT_DECL,
                    clang.cindex.CursorKind.UNION_DECL,
                ]

                kind_enum = node.kind in [
                    clang.cindex.CursorKind.ENUM_DECL,
                ]

                is_new = node.spelling not in classes
                # Fix forward declarations clobbering definitions.
                is_real_def = node.is_definition()

                if kind_ok and is_new and is_real_def:
                    classes[node.spelling] = node

                elif kind_enum and is_new and is_real_def:
                    enums[node.spelling] = node

        # To construct the field map, we will construct the following objects:
        # 1. _classes
        #       Extract a list of all classes in the translation units.
        # 2. _bases
        #       Extract a mapping from class name to all base classes.
        # 3. _fields
        #       Extract a mapping from class name to all the fields,
        #       but base classes not expanded, record types not expanded.
        # 4. _rtti_map
        #       _fields with base classes expanded.
        # 5. field_map
        #       _fields with base classes expanded and record types expanded.
        # 6. enum_map
        #       Extract a mapping of all enumerations in the code base where the value is a
        #       list of (enum_name, enum_value) pairs.

        # _classes : list of all classes in the translation unit
        self._classes: List[clang.cindex.Cursor] = classes.values()
        self._classes = sorted(self._classes, key=lambda node: node.spelling)

        # _bases : class name -> list of base classes for the class, for C++ inheritance.
        self._bases: Mapping[str, List[clang.cindex.Cursor]] = {
            node.spelling: [
                child.referenced
                for child in node.get_children()
                if child.kind == clang.cindex.CursorKind.CXX_BASE_SPECIFIER
            ]
            for node in self._classes
        }

        # _fields : class name -> list of fields in the class
        self._fields: Mapping[str, List[Field]] = {
            node.spelling: [
                Field(
                    child.displayname,
                    child.type.spelling
                    if child.type.get_canonical().kind != clang.cindex.TypeKind.RECORD
                    else child.type.get_canonical().get_declaration().spelling,
                    child.type.get_canonical().kind,
                    field_size=child.type.get_size(),
                )
                for child in node.get_children()
                if child.kind == clang.cindex.CursorKind.FIELD_DECL
            ]
            for node in self._classes
        }

        # _rtti_map : class name ->
        #               list of fields in the class with base classes expanded, for C++ inheritance.
        self._rtti_map: Mapping[str, List[Field]] = {
            node_name: self._construct_base_expanded_fields(node_name) for node_name in self._bases
        }

        # field_map: class name ->
        #               list of fields in the class with base classes expanded
        #               and record types expanded
        self.field_map: Mapping[str, List[Field]] = {
            node_name: self._construct_fully_expanded_fields(node_name, classes, prefix=f"{node_name}_")
            for node_name in self._bases
        }

        self.enum_map: Mapping[str, List[Tuple[str, int]]] = {
            node: [(child.spelling, child.enum_value) for child in enum.get_children()] for node, enum in enums.items()
        }

    def _construct_base_expanded_fields(self, class_name):
        """
        Construct the list of base-class-expanded fields.
        Depends on self._fields and self._bases.

        Base class fields are prepended.

        Parameters
        ----------
        class_name : str
            The name of the class to construct a field list for.

        Returns
        -------
        A base-class-expanded list of fields for the input class.
        """
        fields, bases = self._fields, self._bases

        # If the class has no fields, we are done.
        field_list = fields.get(class_name)
        if field_list is None:
            return []

        # Otherwise, if there are any base classes,
        # recursively prepend the fields from the base classes,
        # and then return the fields for this class.
        base_classes = bases[class_name]
        for base_class in base_classes:
            base = self._construct_base_expanded_fields(base_class.spelling)
            field_list = base + field_list
        return field_list

    def _construct_fully_expanded_fields(self, class_name, classes, prefix=""):
        """
        Construct the list of base-class- and record-type- expanded fields.
        Depends on self._rtti_map.

        Parameters
        ----------
        class_name : str
            The name of the class to construct a field list for.

        prefix : str
            Recursive helper parameter, will be prefixed onto field names.

        Returns
        -------
        A base-class- and record-type- expanded list of fields for the class.

        Warnings
        --------
        Record types are only expanded wherever possible, and are otherwise
        dropped after printing a warning.
        """
        rtti_map = self._rtti_map

        fields = rtti_map[class_name]
        new_fields = []
        # For every field in the base-class-expanded field list for the class,
        for field in fields:
            if field.canonical_type_kind != clang.cindex.TypeKind.RECORD:
                # If the field is not a record type,
                # just append the field to the list of new fields.
                new_field = Field(
                    name=f"{prefix}{field.name}",
                    pg_type=field.pg_type,
                    canonical_type_kind=field.canonical_type_kind,
                    is_padding=field.is_padding,
                    field_size=field.field_size,
                )
                new_fields.append(new_field)
            else:
                # If the field is a record type, try adding the list of
                # base-class- and record-type- expanded fields.
                # However, this is not always possible,
                # e.g., for non-PostgreSQL structs.
                if field.pg_type not in rtti_map:
                    logger.warning("No type info for %s used in %s.", field.pg_type, class_name)
                else:
                    expanded_fields = self._construct_fully_expanded_fields(
                        field.pg_type, classes, prefix=prefix + f"{field.name}_"
                    )

                    new_fields.extend(expanded_fields)

                    # This is a big band-aid, aimed at temporarily patching the behavior of hutch/TScout when
                    # it comes to struct alignments. As of this change (Mar 7, 2022), we have that BPF ->
                    # Python userspace structs don't respect __attribute__((aligned(#))). It doesn't seem like
                    # ctypes has a way of specifying per-field alignment either...
                    #
                    # We can theoretically generate structs for the python collector to read but then Hutch would
                    # still segfault with the incorrect offsets. The following attempts to compute whether or not
                    # there will be padding between the last data variable and the end of the struct.
                    #
                    # If there is a difference, a manual "padding" field is inserted.
                    struct_size = classes[field.pg_type].type.get_size()
                    last_field = rtti_map[field.pg_type][-1]
                    last_field_offset = int(classes[field.pg_type].type.get_offset(last_field.name) / 8)
                    last_field_data = last_field.field_size + last_field_offset
                    if struct_size - last_field_data > 0:
                        padding = struct_size - last_field_data
                        new_fields.append(
                            Field(
                                name=f"{prefix}{field.name}_padding",
                                pg_type=None,
                                canonical_type_kind=clang.cindex.TypeKind.CONSTANTARRAY,
                                field_size=padding,
                                is_padding=True,
                            )
                        )
        new_fields[0].alignment = classes[class_name].type.get_align()

        # The alignment value is the struct's alignment, not the field. We assign this to the first field of a
        # struct since the address of a struct and its first field must be the same since their memory addresses must be
        # the same.
        return new_fields
