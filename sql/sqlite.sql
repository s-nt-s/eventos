CREATE INDEX idx_tabla_year ON MOVIE(year);
CREATE INDEX idx_director_person ON DIRECTOR(person);
/*
CREATE INDEX idx_worker_person ON WORKER(person);
CREATE INDEX idx_worker_category ON WORKER(category);
*/
CREATE INDEX idx_person_name_nocase ON PERSON(name COLLATE NOCASE);
CREATE INDEX idx_title_title_nocase ON TITLE(title COLLATE NOCASE);

CREATE VIRTUAL TABLE PERSON_FTS_AUX USING fts5(
    name, 
    content='PERSON', 
    content_rowid='rowid', 
    tokenize = 'unicode61 remove_diacritics 2'
);
INSERT INTO PERSON_FTS_AUX(rowid, name)
SELECT rowid, name FROM PERSON;
CREATE VIEW PERSON_FTS AS
SELECT t.id, f.name
FROM PERSON_FTS_AUX f
JOIN PERSON t ON t.rowid = f.rowid
;

CREATE VIRTUAL TABLE TITLE_FTS_AUX USING fts5(
    title, 
    content='TITLE', 
    content_rowid='rowid', 
    tokenize = 'unicode61 remove_diacritics 2'
);

INSERT INTO TITLE_FTS_AUX(rowid, title)
SELECT rowid, title FROM TITLE;
CREATE VIEW TITLE_FTS AS
SELECT t.movie, f.title
FROM TITLE_FTS_AUX f
JOIN TITLE t ON t.rowid = f.rowid
;

VACUUM;
