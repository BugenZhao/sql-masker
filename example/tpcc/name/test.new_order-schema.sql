/*from(br)*/CREATE TABLE `new_order` (
  `no_o_id` int(11) NOT NULL,
  `no_d_id` int(11) NOT NULL,
  `no_w_id` int(11) NOT NULL,
  PRIMARY KEY (`no_w_id`,`no_d_id`,`no_o_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
