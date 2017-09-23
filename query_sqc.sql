DROP TABLE car CASCADE CONSTRAINTS;

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

CREATE TABLE driver (
  id            NUMBER(3) PRIMARY KEY,
  first_name    VARCHAR2(32) NOT NULL,
  second_name   VARCHAR2(32) NOT NULL,
  birthday_date DATE
);

INSERT INTO driver VALUES (1, 'vasya', 'ivanov', {d '1992-01-01'});

/*For Lab6*/
DROP TABLE countries CASCADE CONSTRAINTS;

CREATE TABLE countries (
  id         NUMBER(3) CONSTRAINT PK PRIMARY KEY,
  name       VARCHAR2(64) NOT NULL,
  population NUMBER(12) NOT NULL
);

INSERT INTO countries VALUES (1, 'Russia', 150000);
INSERT INTO countries VALUES (2, 'Japan', 350000);
INSERT INTO countries VALUES (3, 'China', 1350000);
INSERT INTO countries VALUES (4, 'India', 1180000);

/*For Lab7*/
SELECT ГРУППА, ФАМИЛИЯ, ИМЯ, ОТЧЕСТВО, ДАТА_РОЖДЕНИЯ, МЕСТО_РОЖДЕНИЯ
FROM
    Н_ЛЮДИ
    JOIN
    Н_УЧЕНИКИ
        ON
            Н_ЛЮДИ.ИД = Н_УЧЕНИКИ.ЧЛВК_ИД
        WHERE
            ГРУППА = 4108;

/*For Lab8 (Dima)*/
SELECT "ФАМ. (ИМЯ или ОТЧ.)", "COUNT"
FROM (
  SELECT ФАМИЛИЯ "ФАМ. (ИМЯ или ОТЧ.)", COUNT(ФАМИЛИЯ) "COUNT"
  FROM
    Н_ЛЮДИ
      GROUP BY ФАМИЛИЯ
      HAVING COUNT(ФАМИЛИЯ) >= 50
    UNION
  SELECT ИМЯ, COUNT(ИМЯ)
  FROM
    Н_ЛЮДИ
      GROUP BY ИМЯ
      HAVING COUNT(ИМЯ) >= 300
    UNION
  SELECT ОТЧЕСТВО, COUNT(ОТЧЕСТВО)
  FROM
    Н_ЛЮДИ
      GROUP BY ОТЧЕСТВО
      HAVING COUNT(ОТЧЕСТВО) >= 300
)
ORDER BY "COUNT";
