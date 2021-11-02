/*from(br)*/CREATE TABLE `district` (
  `d_id` int(11) NOT NULL,
  `d_w_id` int(11) NOT NULL,
  `d_name` varchar(10) DEFAULT NULL,
  `d_street_1` varchar(20) DEFAULT NULL,
  `d_street_2` varchar(20) DEFAULT NULL,
  `d_city` varchar(20) DEFAULT NULL,
  `d_state` char(2) DEFAULT NULL,
  `d_zip` char(9) DEFAULT NULL,
  `d_tax` decimal(4,4) DEFAULT NULL,
  `d_ytd` decimal(12,2) DEFAULT NULL,
  `d_next_o_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`d_w_id`,`d_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
