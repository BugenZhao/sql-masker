PREPARE stmt1
FROM
    'SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?';

SET
    @num = 5;
