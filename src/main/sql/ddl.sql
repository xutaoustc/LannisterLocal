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
                                        `app_id` bigint(20) DEFAULT NULL,
                                        PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for app_heuristic_result_details
-- ----------------------------
DROP TABLE IF EXISTS `app_heuristic_result_details`;
CREATE TABLE `app_heuristic_result_details` (
                                                `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                                `name` varchar(100) DEFAULT NULL,
                                                `value` varchar(100) DEFAULT NULL,
                                                `details` varchar(255) DEFAULT NULL,
                                                `heuristic_id` bigint(20) DEFAULT NULL,
                                                PRIMARY KEY (`id`)
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
                              PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;