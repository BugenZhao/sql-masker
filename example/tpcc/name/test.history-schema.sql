/*from(br)*/CREATE TABLE `history` (
  `h_c_id` int(11) NOT NULL,
  `h_c_d_id` int(11) NOT NULL,
  `h_c_w_id` int(11) NOT NULL,
  `h_d_id` int(11) NOT NULL,
  `h_w_id` int(11) NOT NULL,
  `h_date` datetime DEFAULT NULL,
  `h_amount` decimal(6,2) DEFAULT NULL,
  `h_data` varchar(24) DEFAULT NULL,
  KEY `idx_h_w_id` (`h_w_id`),
  KEY `idx_h_c_w_id` (`h_c_w_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
