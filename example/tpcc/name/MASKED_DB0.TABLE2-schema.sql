/*from(br)*/CREATE TABLE `TABLE2` (
  `COL101_0` int(11) NOT NULL,
  `COL101_1` int(11) NOT NULL,
  `COL101_2` int(11) NOT NULL,
  `COL101_3` int(11) NOT NULL,
  `COL101_4` int(11) NOT NULL,
  `COL101_5` datetime DEFAULT NULL,
  `COL101_6` decimal(6,2) DEFAULT NULL,
  `COL101_7` varchar(24) DEFAULT NULL,
  KEY `idx_h_w_id` (`COL101_4`),
  KEY `idx_h_c_w_id` (`COL101_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
