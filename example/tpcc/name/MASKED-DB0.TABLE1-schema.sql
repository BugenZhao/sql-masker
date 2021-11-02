/*from(br)*/CREATE TABLE `TABLE1` (
  `COL0` int(11) NOT NULL,
  `COL1` int(11) NOT NULL,
  `COL2` varchar(10) DEFAULT NULL,
  `COL3` varchar(20) DEFAULT NULL,
  `COL4` varchar(20) DEFAULT NULL,
  `COL5` varchar(20) DEFAULT NULL,
  `COL6` char(2) DEFAULT NULL,
  `COL7` char(9) DEFAULT NULL,
  `COL8` decimal(4,4) DEFAULT NULL,
  `COL9` decimal(12,2) DEFAULT NULL,
  `COL10` int(11) DEFAULT NULL,
  PRIMARY KEY (`COL1`,`COL0`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
