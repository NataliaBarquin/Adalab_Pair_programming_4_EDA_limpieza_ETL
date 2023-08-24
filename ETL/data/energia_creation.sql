-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema energia
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema energia
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `energia` DEFAULT CHARACTER SET utf8 ;
USE `energia` ;

-- -----------------------------------------------------
-- Table `energia`.`fechas`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`fechas` (
  `id_date` INT NOT NULL,
  `date` DATE NOT NULL,
  PRIMARY KEY (`id_date`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`comunidades`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`comunidades` (
  `id_location` INT NOT NULL,
  `location` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_location`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`comunidades_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`comunidades_renovable_no_renovable` (
  `id_location` INT NOT NULL,
  `value` FLOAT NULL,
  `percentage` FLOAT NULL,
  `energy_type` VARCHAR(45) NULL,
  `comunidades_id_location` INT NOT NULL,
  `fechas_id_date1` INT NOT NULL,
  PRIMARY KEY (`id_location`),
  INDEX `fk_comunidades_renovable_no_renovable_comunidades1_idx` (`comunidades_id_location` ASC) VISIBLE,
  INDEX `fk_comunidades_renovable_no_renovable_fechas1_idx` (`fechas_id_date1` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
    FOREIGN KEY (`comunidades_id_location`)
    REFERENCES `energia`.`comunidades` (`id_location`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_fechas1`
    FOREIGN KEY (`fechas_id_date1`)
    REFERENCES `energia`.`fechas` (`id_date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`nacional_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`nacional_renovable_no_renovable` (
  `idnacional_renovable_no_renovable` INT NOT NULL,
  `value` FLOAT NULL,
  `percentage` FLOAT NULL,
  `nacional_renovable_no_renovablecol` VARCHAR(45) NULL,
  `energy_type` VARCHAR(45) NULL,
  `fechas_id_date` INT NOT NULL,
  PRIMARY KEY (`idnacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fechas1_idx` (`fechas_id_date` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fechas1`
    FOREIGN KEY (`fechas_id_date`)
    REFERENCES `energia`.`fechas` (`id_date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
