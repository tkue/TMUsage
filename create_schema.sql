-------------------------------
-- CREATE SCHEMA FOR SQLITE3 --
-------------------------------

CREATE TABLE IF NOT EXISTS `UsageVoice` (
	`VoiceUsageID`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`TMobileNumber`	TEXT NOT NULL,
	`Date`	TEXT,
	`Time`	TEXT,
	`Destination`	TEXT,
	`NumberConnected`	TEXT,
	`Minutes`	INTEGER,
	`Call Type`	TEXT,
	`OverageCharge`	TEXT,
    `Date_Added_UTC` TEXT NOT NULL,
    `Date_Last_Update_UTC` TEXT NOT NULL
);

CREATE INDEX `IX_UsageVoice` ON `UsageVoice` (`TMobileNumber` ,`NumberConnected` );

CREATE TABLE IF NOT EXISTS CallInfo (
	`CallInfoID` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`ContactID` INTEGER NULL,
	`ReverseLookup` TEXT NULL,
	`Notes` BLOB NULL,
    `Date_Added_UTC` TEXT NOT NULL,
    `Date_Last_Update_UTC` TEXT NOT NULL
);

CREATE INDEX IX_CallInfo ON CallInfo ( ContactID);

IF NOT EXISTS CREATE TABLE Contact (
	`ContactID` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`FirstName` TEXT NULL,
	`LastName` TEXT NULL,
	`PhoneNumber` TEXT NULL,
	`Notes` BLOB,
    `Date_Added_UTC` TEXT NOT NULL,
    `Date_Last_Update_UTC` TEXT NOT NULL
);

ALTER TABLE CallInfo
ADD CONSTRAINT FK_ContacID_Contact
FOREIGN KEY (ContactID)
REFERENCES Contact(ContactID)