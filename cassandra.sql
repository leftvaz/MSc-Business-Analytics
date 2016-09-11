/************************ NotSpotify LTD **************************/

/***********Keyspace***********/
CREATE KEYSPACE BAPT1516 WITH replication = {
'class': 'SimpleStrategy', 'replication_factor':
1} ;

USE BAPT1516;



/***********New types***********/
CREATE TYPE payment (
  option text,
  card_number text,
  balance decimal
);



/***********Question 1***********/
CREATE TABLE UsersByName (
	Username text PRIMARY KEY, 
	Address text, 
	PaymentInformation payment 
) WITH comment='Q1: users by name';

SELECT *
FROM UsersByName
WHERE Username='1';



/***********Question 2***********/
CREATE TABLE SongsByName (
	SongName text, 
	Artist text, 
	Album text, 
	Genre text, 
	Year bigint, 
	Song blob,
	PRIMARY KEY (SongName, Artist)
) WITH comment='Q2: songs by name';

SELECT *
FROM SongsByName
WHERE SongName='a';



/***********Question 3***********/
CREATE TABLE SongsPlayed (
	Username text,
	SongName text,
	time_heard timestamp,
	PRIMARY KEY (Username,time_heard)
) WITH CLUSTERING ORDER BY (time_heard DESC) AND comment='Q3: songs played by a user';

SELECT *
FROM SongsPlayed
WHERE Username='1';




/***********Question 4***********/
CREATE TABLE PlaylistsByName (
	PlaylistName text PRIMARY KEY,
	Description text,
) WITH comment='Q4: playlists by name';

SELECT * 
FROM PlaylistsByName
WHERE PlaylistName='Rock';




/***********Question 5***********/
CREATE TABLE PlaylistsByGenre (
	PlaylistName text,
	Description text,
	Genre text,
	PRIMARY KEY (PlaylistName,Genre)
) WITH comment='Q5: playlists by genre';

CREATE INDEX ON PlaylistsByGenre (Genre);

SELECT * 
FROM PlaylistsByGenre
WHERE Genre='Rock';


/***********Question 6***********/
CREATE TABLE PlaylistsByCreator (
	PlaylistName text,
	Description text,
	Creator text,
	PRIMARY KEY (PlaylistName, Creator)
) WITH comment='playlists by creator';

CREATE INDEX ON PlaylistsByCreator (Creator);

SELECT *
FROM PlaylistsByCreator
WHERE Creator='a';



/***********Question 7***********/
CREATE TABLE PlaylistFollowers (
	PlaylistName text,
	FollowerName text,
	PRIMARY KEY (PlaylistName, FollowerName)
) WITH comment='list of followers for a playlist';

SELECT *
FROM PlaylistFollowers
WHERE PlaylistName='abc';


/***********Question 8***********/
CREATE TABLE UserFollowers (
	Username text,
	FollowerName text,
	PRIMARY KEY (Username, FollowerName)
) WITH comment='list of followers for a user';

SELECT *
FROM UserFollowers
WHERE Username='a';




/***********Question 9***********/
CREATE TABLE SongsInPlaylist (
	PlaylistName text,
	SongName text,
	Artist text,
	PRIMARY KEY(PlaylistName, SongName, Artist)
) WITH comment='list of songs for a playlist';

SELECT *
FROM SongsInPlaylist
WHERE PlaylistName='abc';



/***********Question 10***********/
CREATE TABLE PlaylistTimesPlayed (
	PlaylistName text PRIMARY KEY,
	TimesPlayed bigint
) WITH comment='number of plays for a playlist';

SELECT *
FROM PlaylistTimesPlayed
WHERE PlaylistName='abc';




/***********Question 11***********/
CREATE TABLE SongTimesPlayed (
	SongName text,
	Artist text,
	TimesPlayed bigint,
	PRIMARY KEY (SongName, Artist)
) WITH comment='number of plays for a song';

CREATE INDEX ON SongTimesPlayed (Artist);

SELECT *
FROM SongTimesPlayed
WHERE SongName='1' AND Artist='a';



/***********Question 12***********/
CREATE TABLE PlaylistPopularity (
	PlaylistName text ,
	TimesPlayed bigint,
	cheating bigint,
	PRIMARY KEY ((cheating),TimesPlayed, PlaylistName)
)WITH CLUSTERING ORDER BY (TimesPlayed DESC) AND comment='playlists by descending popularity';

INSERT INTO PlaylistPopularity(PlaylistName, TimesPlayed, cheating) VALUES ('abc',1500, 1);
INSERT INTO PlaylistPopularity(PlaylistName, TimesPlayed, cheating) VALUES ('blah',1700, 1);
INSERT INTO PlaylistPopularity(PlaylistName, TimesPlayed, cheating) VALUES ('dce',1600, 1);

SELECT *
FROM PlaylistPopularity
WHERE cheating=1;




/***********Question 13***********/
CREATE TABLE UserPopularity (
	Username text ,
	TimesFollowed bigint,
	cheating_again bigint,
	PRIMARY KEY ((cheating_again),TimesFollowed, Username)
)WITH CLUSTERING ORDER BY (TimesFollowed DESC) AND comment='users by descending popularity';

INSERT INTO UserPopularity(Username, TimesFollowed, cheating_again) VALUES ('abc',1500, 1);
INSERT INTO UserPopularity(Username, TimesFollowed, cheating_again) VALUES ('blah',1700, 1);
INSERT INTO UserPopularity(Username, TimesFollowed, cheating_again) VALUES ('dce',1600, 1);

SELECT *
FROM UserPopularity
WHERE cheating_again=1;


