/*from(br)*/CREATE TABLE `TABLE5` (
  `COL107_0` int(11) NOT NULL,
  `COL107_1` int(11) NOT NULL,
  `COL107_2` int(11) NOT NULL,
  `COL107_3` int(11) NOT NULL,
  `COL107_4` int(11) NOT NULL,
  `COL107_5` int(11) DEFAULT NULL,
  `COL107_6` datetime DEFAULT NULL,
  `COL107_7` int(11) DEFAULT NULL,
  `COL107_8` decimal(6,2) DEFAULT NULL,
  `COL107_9` char(24) DEFAULT NULL,
  PRIMARY KEY (`COL107_2`,`COL107_1`,`COL107_0`,`COL107_3`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;