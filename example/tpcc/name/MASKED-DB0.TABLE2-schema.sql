/*from(br)*/CREATE TABLE `TABLE2` (
  `COL0` int(11) NOT NULL,
  `COL1` int(11) NOT NULL,
  `COL2` int(11) NOT NULL,
  `COL3` int(11) NOT NULL,
  `COL4` int(11) NOT NULL,
  `COL5` datetime DEFAULT NULL,
  `COL6` decimal(6,2) DEFAULT NULL,
  `COL7` varchar(24) DEFAULT NULL,
  KEY `idx_h_w_id` (`COL4`),
  KEY `idx_h_c_w_id` (`COL2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
