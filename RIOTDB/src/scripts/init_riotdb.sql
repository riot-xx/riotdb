DROP DATABASE IF EXISTS riotdb;
CREATE DATABASE riotdb;
USE riotdb;
--- Table cleanup ---
DROP TABLE IF EXISTS Metadata;
DROP TABLE IF EXISTS DB_Sources;
DROP TABLE IF EXISTS View_Reference;


--- Table creation ---
CREATE TABLE DB_Sources (
    db_source_id    INT(6) UNSIGNED AUTO_INCREMENT,
    db_driver       VARCHAR(30) NOT NULL,
    db_name         VARCHAR(30) NOT NULL,
    username        VARCHAR(30),
    password        VARCHAR(64),
    next_vec_index  INT(10),
    PRIMARY KEY  (db_source_id),
    index (db_source_id)
) engine=myisam;


CREATE TABLE Metadata (
    metadata_id     INT(6)  UNSIGNED AUTO_INCREMENT,
    table_name      VARCHAR(40) NOT NULL,
    db_source_id    INT(6)  UNSIGNED ,
    size            VARCHAR(64),
    is_view         INT(2)  UNSIGNED DEFAULT 0,
    ref_counter     INT(6)  UNSIGNED DEFAULT 1,
    sxp_type        SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_obj         SMALLINT(2) UNSIGNED DEFAULT 1,
    sxp_named       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_gp          SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_mark        SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_debug       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_trace       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_spare       SMALLINT(2) UNSIGNED DEFAULT 1,
    sxp_gcgen       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_gccls       SMALLINT(2) UNSIGNED DEFAULT 0,
    PRIMARY KEY     (metadata_id),
---    FOREIGN KEY     (db_source_id) REFERENCES DB_Sources ON DELETE CASCADE,
    INDEX           (metadata_id)
---    index (db_source_id)
) engine=myisam;


CREATE TABLE View_Reference (
    view_id         INT(6) UNSIGNED,
    table1_id       INT(6) UNSIGNED,
    table2_id       INT(6) UNSIGNED,
    PRIMARY KEY     (view_id),
    index           (view_id)
) engine=myisam;


--- Create Triggers ---
DELIMITER |
CREATE TRIGGER increment_next_id AFTER INSERT ON Metadata
   FOR EACH ROW BEGIN
       UPDATE DB_Sources SET next_vec_index = next_vec_index + 1
       WHERE DB_Sources.db_source_id = NEW.db_source_id;
   END;
|

CREATE TRIGGER increment_ref_counter AFTER INSERT ON View_Reference
   FOR EACH ROW BEGIN
       UPDATE Metadata SET ref_counter = ref_counter + 1
       WHERE metadata_id = NEW.table1_id OR metadata_id = NEW.table2_id;
   END;
|

DELIMITER ;


--- Insert Local DB info ---
INSERT INTO DB_Sources VALUE(NULL,'MySQL','riotdb','mysql',NULL,1);

