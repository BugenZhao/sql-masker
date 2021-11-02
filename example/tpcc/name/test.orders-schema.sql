/*from(br)*/CREATE TABLE `orders` (
  `o_id` int(11) NOT NULL,
  `o_d_id` int(11) NOT NULL,
  `o_w_id` int(11) NOT NULL,
  `o_c_id` int(11) DEFAULT NULL,
  `o_entry_d` datetime DEFAULT NULL,
  `o_carrier_id` int(11) DEFAULT NULL,
  `o_ol_cnt` int(11) DEFAULT NULL,
  `o_all_local` int(11) DEFAULT NULL,
  PRIMARY KEY (`o_w_id`,`o_d_id`,`o_id`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_order` (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
