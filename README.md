## fileutils

Small utility functions for working with text files:

- quickly split files into nparts while preserving whole lines
- sort files in parallel by column
- merge sorted file parts into single sorted file

```py
from fileutils import disksort

disksort("data/large.csv", "sorted.csv", col=6)
```

### TODO:

- [ ] create disksort function that uses building blocks
- [ ] implement on-disk joins
- [ ] write tests and docs
- [ ] add support for sorting on multiple columns?