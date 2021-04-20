/*
Navicat MySQL Data Transfer

Source Server         : cmdb2
Source Server Version : 50716
Source Host           : localhost:3306
Source Database       : lannister

Target Server Type    : MYSQL
Target Server Version : 50716
File Encoding         : 65001

Date: 2021-04-20 14:01:49
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for app_heuristic_result
-- ----------------------------
DROP TABLE IF EXISTS `app_heuristic_result`;
CREATE TABLE `app_heuristic_result` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `heuristic_class` varchar(100) DEFAULT NULL,
  `heuristic_name` varchar(100) DEFAULT NULL,
  `severity_id` int(11) DEFAULT NULL,
  `score` int(11) DEFAULT NULL,
  `result_id` bigint(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `result_id` (`result_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for app_heuristic_result_details
-- ----------------------------
DROP TABLE IF EXISTS `app_heuristic_result_details`;
CREATE TABLE `app_heuristic_result_details` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `value` varchar(1000) DEFAULT NULL,
  `details` varchar(255) DEFAULT NULL,
  `heuristic_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `heuristic_id` (`heuristic_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for app_result
-- ----------------------------
DROP TABLE IF EXISTS `app_result`;
CREATE TABLE `app_result` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `app_id` varchar(100) DEFAULT NULL,
  `tracking_url` varchar(255) DEFAULT NULL,
  `queue_name` varchar(100) DEFAULT NULL,
  `username` varchar(100) DEFAULT NULL,
  `start_time` bigint(20) DEFAULT NULL,
  `finish_time` bigint(20) DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `job_type` varchar(100) DEFAULT NULL,
  `resource_used` bigint(20) DEFAULT NULL,
  `total_delay` bigint(20) DEFAULT NULL,
  `resource_wasted` bigint(20) DEFAULT NULL,
  `severity_Id` tinyint(100) DEFAULT NULL,
  `score` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `app_id` (`app_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8;
