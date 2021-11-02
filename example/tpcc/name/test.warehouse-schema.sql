/*from(br)*/CREATE TABLE `warehouse` (
  `w_id` int(11) NOT NULL,
  `w_name` varchar(10) DEFAULT NULL,
  `w_street_1` varchar(20) DEFAULT NULL,
  `w_street_2` varchar(20) DEFAULT NULL,
  `w_city` varchar(20) DEFAULT NULL,
  `w_state` char(2) DEFAULT NULL,
  `w_zip` char(9) DEFAULT NULL,
  `w_tax` decimal(4,4) DEFAULT NULL,
  `w_ytd` decimal(12,2) DEFAULT NULL,
  PRIMARY KEY (`w_id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
