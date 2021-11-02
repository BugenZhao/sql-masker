/*from(br)*/CREATE TABLE `TABLE6` (
  `COL0` int(11) NOT NULL,
  `COL1` int(11) NOT NULL,
  `COL2` int(11) NOT NULL,
  `COL3` int(11) DEFAULT NULL,
  `COL4` datetime DEFAULT NULL,
  `COL5` int(11) DEFAULT NULL,
  `COL6` int(11) DEFAULT NULL,
  `COL7` int(11) DEFAULT NULL,
  PRIMARY KEY (`COL2`,`COL1`,`COL0`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_order` (`COL2`,`COL1`,`COL3`,`COL0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
