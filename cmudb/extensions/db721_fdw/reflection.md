### Strength of DB721 Format
- It is very intuitive and easy to parse.
- The column data is packed as fixed length array in the file, which enables easy sequential access.

### Weakness of DB721 Format
- The file is not blocked and not replicated, which means putting all data in a single file is not reliable. The table cannot grow very large and hard to make query parallelised.
- There is no compression; compression can make the system benefit more from releasing I/O bandwidth.

### Suggestions for Improvement
- Can further break columns down to column blocks and separated to multiple file. A single file can be made as the catalog.
- Apply Rans compression to each column block. Students can be given the quest to write iterator on compressed CB.

### Other Reflections
- The system design would be straightforward given this file format. Keeping multiple fds can incrementatlly materialize would get the best performance out of memory and I/O bandwidth.
- What is difficult is how to find out the attribute number and how to build/extract from the qual expressions. The postgres has nearly no document on this, and there are a lot of pitfalls like locale and text/cstring.
- It is hard to get the memory ref correct in complex kernel developing. A good thing for me is to print trace logs along the path so I quickly know where is the segmentation fault. (gdb is good, but not quicker).
- The palloc() memory reference is impressive. In this way, kernel developers can stop worrying messing up memory, because memory gets released at each end of transaction.

