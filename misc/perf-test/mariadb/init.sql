DROP VIEW IF EXISTS test.constant;
CREATE VIEW test.constant AS SELECT 1;

DROP TABLE IF EXISTS `10k_rows`;
CREATE TABLE `10k_rows` 
(
  `id`       bigint(20)  NOT NULL  AUTO_INCREMENT,
  `name`     varchar(50) NOT NULL,
  `datetime` timestamp   NULL      DEFAULT CURRENT_TIMESTAMP,
  `num`      int(11)               DEFAULT NULL,
  `value`    float                 DEFAULT NULL,
  PRIMARY KEY (`id`)
);

DROP PROCEDURE IF EXISTS generate_10k_rows;
DELIMITER $$
CREATE PROCEDURE generate_10k_rows()
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < 10000 DO
    INSERT INTO `10k_rows` (`name`, `datetime`,`num`, `value`) VALUES (
      FROM_UNIXTIME(UNIX_TIMESTAMP('2020-01-01 00:00:00') + FLOOR(RAND() * 31536000)),
      FROM_UNIXTIME(UNIX_TIMESTAMP('2020-01-01 00:00:00') + FLOOR(RAND() * 31536000)),
      FLOOR(RAND() * 10000),
      ROUND(RAND() * 100, 4)
    );
    SET i = i + 1;
  END WHILE;
END$$
DELIMITER ;

CALL generate_10k_rows();
