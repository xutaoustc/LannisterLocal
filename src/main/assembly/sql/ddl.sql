SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for app_result
-- ----------------------------
DROP TABLE IF EXISTS `app_result`;
CREATE TABLE `app_result` (
                              `id` bigint(20) NOT NULL AUTO_INCREMENT,
                              `app_id` varchar(100) NOT NULL,
                              `tracking_url` varchar(255) NOT NULL,
                              `queue_name` varchar(100) NOT NULL,
                              `username` varchar(100) NOT NULL,
                              `start_time` bigint(20) NOT NULL,
                              `finish_time` bigint(20) NOT NULL,
                              `name` varchar(1000) NOT NULL,
                              `successful_job` BOOLEAN NOT NULL,
                              `job_type` varchar(100) NOT NULL,
                              `severity_Id` tinyint(100) NOT NULL,
                              `score` int(11) NOT NULL,
                              PRIMARY KEY (`id`),
                              UNIQUE KEY `app_id` (`app_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for app_heuristic_result
-- ----------------------------
DROP TABLE IF EXISTS `app_heuristic_result`;
CREATE TABLE `app_heuristic_result` (
                                        `id` bigint(20) NOT NULL AUTO_INCREMENT,
                                        `heuristic_class` varchar(100) NOT NULL,
                                        `heuristic_name` varchar(100) NOT NULL,
                                        `heuristic_severity_id` tinyint(100) NOT NULL,
                                        `score` int(11) NOT NULL,
                                        `details` JSON NOT NULL,
                                        `result_id` bigint(100) NOT NULL,
                                        PRIMARY KEY (`id`),
                                        INDEX result_id(result_id)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;

