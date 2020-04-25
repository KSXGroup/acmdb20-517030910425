# ACM DB Lab 2 Report

- Describe any design decisions you made, including your choice of  page eviction policy. Describe briefly your insertion and deletion  methods in B+ tree. Describe your idea in sovling the bonus exercise (if it applies).

  My LRU cache was finished in Lab 1, and it works well. So no new design was made on caching was made in this lab.

  The insertion of B+ Tree is relatively simpler, I follow the implementation guide and always ensure it is a legal B+ Tree. The thing need carefulness is that the splitting of internal pages and leaf pages follow different rules. 

  The deletion of B+ Tree involves stealing from the sibling and merging the page. Because of the elegant  design of the database architecture, the implement of stealing and merging is easy if you have a clear logic and follow the implementation guide carefully. 

- Discuss and justify any changes you made to the API.

  Instead of using explicit evict function, I  embedded the evicting into my LRU cache. It can write extra pages to the disk and remove it from the buffer. So the `evict()` function is abandoned. And there is no other change to the API.

- Describe any missing or incomplete elements of your code.

  All part is finished in this lab, and all test are passed.

- Describe how long you spent on the lab, and whether there was anything you found particularly difficult or confusing.

  The time I spent on this lab add up to about 3 days.

 