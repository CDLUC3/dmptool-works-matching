"""MySQL testcontainer fixtures for DMSP loader integration tests."""

import pymysql
import pymysql.cursors
import pytest
from testcontainers.mysql import MySqlContainer

MYSQL_IMAGE = "mysql:8.0"

SCHEMA_SQL = """
CREATE TABLE `works` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `doi` varchar(255) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_doi` (`doi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `workVersions` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `workId` int unsigned NOT NULL,
  `hash` binary(16) NOT NULL,
  `workType` varchar(255) NOT NULL,
  `publicationDate` date DEFAULT NULL,
  `title` text,
  `abstractText` mediumtext,
  `authors` json NOT NULL,
  `institutions` json NOT NULL,
  `funders` json NOT NULL,
  `awards` json NOT NULL,
  `publicationVenue` varchar(1000) DEFAULT NULL,
  `sourceName` varchar(255) NOT NULL,
  `sourceUrl` varchar(255) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_hash` (`workId`,`hash`),
  CONSTRAINT `fk_workVersions_works_workId` FOREIGN KEY (`workId`) REFERENCES `works` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `plans` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `dmpId` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `plans_dmpid_idx` (`dmpId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `relatedWorks` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `planId` int unsigned NOT NULL,
  `workVersionId` int unsigned NOT NULL,
  `score` float NOT NULL,
  `status` varchar(256) NOT NULL DEFAULT 'PENDING',
  `doiMatch` json DEFAULT NULL,
  `contentMatch` json DEFAULT NULL,
  `authorMatches` json DEFAULT NULL,
  `institutionMatches` json DEFAULT NULL,
  `funderMatches` json DEFAULT NULL,
  `awardMatches` json DEFAULT NULL,
  `scoreMax` float NOT NULL,
  `sourceType` varchar(32) NOT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_planId_workVersionId` (`planId`,`workVersionId`),
  CONSTRAINT `fk_relatedWorks_plans_planId` FOREIGN KEY (`planId`) REFERENCES `plans` (`id`),
  CONSTRAINT `fk_relatedWorks_workVersions_workVersionId` FOREIGN KEY (`workVersionId`) REFERENCES `workVersions` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""

# Real procedure from dmsp_backend_prototype — creates TEMPORARY staging tables.
CREATE_STAGING_PROC_SQL = """
CREATE PROCEDURE `create_related_works_staging_tables`()
BEGIN
  DROP TEMPORARY TABLE IF EXISTS stagingWorkVersions;
  CREATE TEMPORARY TABLE stagingWorkVersions
  (
    `doi`              VARCHAR(255) NOT NULL PRIMARY KEY,
    `hash`             BINARY(16)   NOT NULL,
    `workType`         VARCHAR(255) NOT NULL,
    `publicationDate`  DATE         NULL,
    `title`            TEXT         NULL,
    `abstractText`     MEDIUMTEXT   NULL,
    `authors`          JSON         NOT NULL,
    `institutions`     JSON         NOT NULL,
    `funders`          JSON         NOT NULL,
    `awards`           JSON         NOT NULL,
    `publicationVenue` VARCHAR(1000) NULL,
    `sourceName`       VARCHAR(255) NOT NULL,
    `sourceUrl`        VARCHAR(255) NOT NULL
  ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci;

  DROP TEMPORARY TABLE IF EXISTS stagingRelatedWorks;
  CREATE TEMPORARY TABLE stagingRelatedWorks
  (
    `id`                 INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `planId`             INT UNSIGNED NULL,
    `dmpDoi`             VARCHAR(255) NULL,
    `workDoi`            VARCHAR(255) NOT NULL,
    `hash`               BINARY(16)   NOT NULL,
    `sourceType`         VARCHAR(32)  NOT NULL,
    `score`              FLOAT        NOT NULL,
    `status`             VARCHAR(255) NOT NULL,
    `scoreMax`           FLOAT        NOT NULL,
    `doiMatch`           JSON         NOT NULL,
    `contentMatch`       JSON         NOT NULL,
    `authorMatches`      JSON         NOT NULL,
    `institutionMatches` JSON         NOT NULL,
    `funderMatches`      JSON         NOT NULL,
    `awardMatches`       JSON         NOT NULL,

    INDEX (`planId`, `dmpDoi`, `workDoi`),
    CONSTRAINT unique_hash UNIQUE (`planId`, `dmpDoi`, `workDoi`),
    CONSTRAINT one_of_plan_id_dmp_doi_not_null CHECK (planId IS NOT NULL OR dmpDoi IS NOT NULL)
  ) ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4
    COLLATE = utf8mb4_0900_ai_ci;
END
"""

# Simplified stub: copies staging data into permanent works + workVersions tables.
BATCH_UPDATE_PROC_SQL = """
CREATE PROCEDURE `batch_update_related_works`(IN systemMatched BOOLEAN)
BEGIN
  INSERT IGNORE INTO works (doi)
  SELECT doi FROM stagingWorkVersions;

  INSERT IGNORE INTO workVersions (workId, hash, workType, publicationDate, title,
                                   abstractText, authors, institutions, funders,
                                   awards, publicationVenue, sourceName, sourceUrl)
  SELECT w.id, s.hash, s.workType, s.publicationDate, s.title, s.abstractText,
         s.authors, s.institutions, s.funders, s.awards, s.publicationVenue,
         s.sourceName, s.sourceUrl
  FROM stagingWorkVersions s
  INNER JOIN works w ON s.doi = w.doi;
END
"""

CLEANUP_PROC_SQL = """
CREATE PROCEDURE `cleanup_orphan_works`()
BEGIN
  SELECT 1;
END
"""


@pytest.fixture(scope="session")
def mysql_container():
    """Start a MySQL 8.0 container for the test session."""
    with MySqlContainer(MYSQL_IMAGE) as container:
        yield container


@pytest.fixture
def mysql_conn(mysql_container):
    """Create a pymysql connection with schema and stored procedures.

    Tables and procedures are created fresh for each test, then dropped after.
    """
    conn = pymysql.connect(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        user=mysql_container.username,
        password=mysql_container.password,
        database=mysql_container.dbname,
        cursorclass=pymysql.cursors.DictCursor,
    )

    with conn.cursor() as cursor:
        for statement in SCHEMA_SQL.split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        cursor.execute(CREATE_STAGING_PROC_SQL)
        cursor.execute(BATCH_UPDATE_PROC_SQL)
        cursor.execute(CLEANUP_PROC_SQL)
    conn.commit()

    yield conn

    # Teardown: drop everything so the next test starts clean
    with conn.cursor() as cursor:
        cursor.execute("DROP PROCEDURE IF EXISTS create_related_works_staging_tables")
        cursor.execute("DROP PROCEDURE IF EXISTS batch_update_related_works")
        cursor.execute("DROP PROCEDURE IF EXISTS cleanup_orphan_works")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in ("relatedWorks", "workVersions", "works", "plans"):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    conn.close()
