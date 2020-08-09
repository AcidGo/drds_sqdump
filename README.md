# drds_sqdump
DRDS 专有云版中的序列（sequence）属于 drds 逻辑实例概念，在使用 mysqldump 备份时无法获取到 show sequences 的值，并且序列的到处需要一些推导，错开已用端区。sq(sequence)备份小工具。
