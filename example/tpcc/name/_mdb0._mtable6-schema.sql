/*from(br)*/CREATE TABLE `_mtable6` (
  `_mcol1jlsq6h` int(11) NOT NULL,
  `_mcololy660` int(11) NOT NULL,
  `_mcol1hc2rfw` int(11) NOT NULL,
  `_mcol1ygy5mr` int(11) DEFAULT NULL,
  `_mcol1oyxzd4` datetime DEFAULT NULL,
  `_mcolgf2z3e` int(11) DEFAULT NULL,
  `_mcolrvj7y4` int(11) DEFAULT NULL,
  `_mcolekaypy` int(11) DEFAULT NULL,
  PRIMARY KEY (`_mcol1hc2rfw`,`_mcololy660`,`_mcol1jlsq6h`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_order` (`_mcol1hc2rfw`,`_mcololy660`,`_mcol1ygy5mr`,`_mcol1jlsq6h`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
