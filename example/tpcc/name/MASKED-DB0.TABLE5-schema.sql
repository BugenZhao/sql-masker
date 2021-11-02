/*from(br)*/CREATE TABLE `TABLE5` (
  `COL0` int(11) NOT NULL,
  `COL1` int(11) NOT NULL,
  `COL2` int(11) NOT NULL,
  `COL3` int(11) NOT NULL,
  `COL4` int(11) NOT NULL,
  `COL5` int(11) DEFAULT NULL,
  `COL6` datetime DEFAULT NULL,
  `COL7` int(11) DEFAULT NULL,
  `COL8` decimal(6,2) DEFAULT NULL,
  `COL9` char(24) DEFAULT NULL,
  PRIMARY KEY (`COL2`,`COL1`,`COL0`,`COL3`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
