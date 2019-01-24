DROP DATABASE metadata;
CREATE DATABASE metadata;

USE metadata;

CREATE TABLE host (
    id INT NOT NULL PRIMARY KEY auto_increment,
    host VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE `database` (
    id INT NOT NULL PRIMARY KEY auto_increment,
    host_id INT NOT NULL,
    `database` VARCHAR(255) NOT NULL,
    CONSTRAINT UNIQUE(host_id, `database`),
    FOREIGN KEY fk_host(host_id)
    REFERENCES host (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE `table` (
    id INT NOT NULL PRIMARY KEY auto_increment,
    database_id INT NOT NULL,
    `table` VARCHAR(255) NOT NULL,
    CONSTRAINT UNIQUE(database_id, `table`),
    FOREIGN KEY fk_database(database_id)
    REFERENCES `database` (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE `column` (
    id INT NOT NULL PRIMARY KEY auto_increment,
    table_id INT NOT NULL,
    `column` VARCHAR(255) NOT NULL,
    CONSTRAINT UNIQUE(table_id, `column`),
    nullable VARCHAR(10) NOT NULL,
    type VARCHAR(15) NOT NULL,
    `default` VARCHAR(255) NOT NULL,
    FOREIGN KEY fk_table(table_id)
    REFERENCES `table` (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE `foreign keys` (
    id INT NOT NULL PRIMARY KEY auto_increment,
    foreign_key_id INT NOT NULL,
    column1_id INT NOT NULL,
    column2_id INT NOT NULL,
    CONSTRAINT UNIQUE(foreign_key_id, column1_id, column2_id),
    FOREIGN KEY fk_column1(column1_id)
    REFERENCES `column` (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
    FOREIGN KEY fk_column2(column2_id)
    REFERENCES `column` (id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);