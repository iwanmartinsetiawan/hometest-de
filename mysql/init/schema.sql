CREATE DATABASE IF NOT EXISTS dev;
USE dev;

CREATE TABLE IF NOT EXISTS patients (
    _id VARCHAR(24) PRIMARY KEY,
    profession VARCHAR(255),
    fullName VARCHAR(255),
    gender VARCHAR(1),
    bornDate DATE,
    mobileNumber VARCHAR(15),
    nik VARCHAR(16),
    channel VARCHAR(50),
    createdAt DATETIME,
    updatedAt DATETIME,
    vaccineDate DATE,
    vaccineLocationName VARCHAR(255),
    faskesCode VARCHAR(50),
    vaccineCode VARCHAR(50),
    vaccineStatus INT,
    type VARCHAR(50)
)
;