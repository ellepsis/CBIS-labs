DROP TABLE car CASCADE CONSTRAINTS ;

CREATE TABLE car (
  id           NUMBER(3) PRIMARY KEY,
  model        VARCHAR2(64) NOT NULL,
  release_year NUMBER(4)
);

DROP TABLE car_rent;

CREATE TABLE car_rent (
  id           NUMBER(6) PRIMARY KEY,
  start_date   DATE,
  end_date     DATE,
  rent_comment VARCHAR2(1024),
  car_id       NUMBER(3) REFERENCES car (id)
);

INSERT INTO CAR_RENT VALUES (1, {d '2000-9-15'}, {d '2000-9-15'}, 'comment 1', 1);