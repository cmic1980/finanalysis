-- --------------------------------------------------------
-- 主机:                           119.8.103.176
-- 服务器版本:                        10.5.5-MariaDB-1:10.5.5+maria~focal - mariadb.org binary distribution
-- 服务器操作系统:                      debian-linux-gnu
-- HeidiSQL 版本:                  10.3.0.5771
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- 导出 stock 的数据库结构
CREATE DATABASE IF NOT EXISTS `stock` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
USE `stock`;

-- 导出  表 stock.esp 结构
CREATE TABLE IF NOT EXISTS `eps` (
  `symbol` char(6) NOT NULL,
  `type` varchar(10) NOT NULL,
  `s0` decimal(10,4) DEFAULT NULL,
  `s1` decimal(10,4) DEFAULT NULL,
  `s2` decimal(10,4) DEFAULT NULL,
  `s3` decimal(10,4) DEFAULT NULL,
  `s4` decimal(10,4) DEFAULT NULL,
  `s5` decimal(10,4) DEFAULT NULL,
  `s6` decimal(10,4) DEFAULT NULL,
  `s7` decimal(10,4) DEFAULT NULL,
  `s8` decimal(10,4) DEFAULT NULL,
  `s9` decimal(10,4) DEFAULT NULL,
  `s10` decimal(10,4) DEFAULT NULL,
  PRIMARY KEY (`symbol`,`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 数据导出被取消选择。

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
