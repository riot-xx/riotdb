drop table if exists ge_solve_A;
drop table if exists ge_solve_b;
drop table if exists ge_solve_x;
create table ge_solve_A(i int, j int, v double);
create table ge_solve_b(i int, v double);
create table ge_solve_x(i int, v double);

DELETE FROM ge_solve_A;
INSERT INTO ge_solve_A VALUES(1, 1, 1);
INSERT INTO ge_solve_A VALUES(1, 2, 2);
INSERT INTO ge_solve_A VALUES(1, 3, 3);
INSERT INTO ge_solve_A VALUES(1, 4, 4);
INSERT INTO ge_solve_A VALUES(2, 1, 5);
INSERT INTO ge_solve_A VALUES(2, 2, 7);
INSERT INTO ge_solve_A VALUES(2, 3, 11);
INSERT INTO ge_solve_A VALUES(2, 4, 13);
INSERT INTO ge_solve_A VALUES(3, 1, 13);
INSERT INTO ge_solve_A VALUES(3, 2, 17);
INSERT INTO ge_solve_A VALUES(3, 3, 19);
INSERT INTO ge_solve_A VALUES(3, 4, 23);
DELETE FROM ge_solve_b;
INSERT INTO ge_solve_b VALUES(1, 362);
INSERT INTO ge_solve_b VALUES(2, 1182);
INSERT INTO ge_solve_b VALUES(3, 2118);
DELETE FROM ge_solve_x;
CALL ge_solve(3, 4);
SELECT * FROM ge_solve_x ORDER BY i;
-- ans =
-- 
--   -86
--    92
--    88
--     0

DELETE FROM ge_solve_A;
INSERT INTO ge_solve_A VALUES(1, 1, 1);
INSERT INTO ge_solve_A VALUES(1, 2, 2);
INSERT INTO ge_solve_A VALUES(1, 3, 3);
INSERT INTO ge_solve_A VALUES(2, 1, 5);
INSERT INTO ge_solve_A VALUES(2, 2, 7);
INSERT INTO ge_solve_A VALUES(2, 3, 11);
INSERT INTO ge_solve_A VALUES(3, 1, 13);
INSERT INTO ge_solve_A VALUES(3, 2, 17);
INSERT INTO ge_solve_A VALUES(3, 3, 19);
DELETE FROM ge_solve_b;
INSERT INTO ge_solve_b VALUES(1, 3402);
INSERT INTO ge_solve_b VALUES(2, 12486);
INSERT INTO ge_solve_b VALUES(3, 24342);
DELETE FROM ge_solve_x;
CALL ge_solve(3, 3);
SELECT * FROM ge_solve_x ORDER BY i;
-- ans =
-- 
--    123.00
--    456.00
--    789.00

drop table if exists ge_solve_A;
drop table if exists ge_solve_b;
drop table if exists ge_solve_x;

--

drop table if exists gj_invert_A;
drop table if exists gj_invert_B;
create table gj_invert_A(i int, j int, v double);
create table gj_invert_B(i int, j int, v double);

delete from gj_invert_A;
insert into gj_invert_A values(1, 1, 1);
insert into gj_invert_A values(1, 2, 2);
insert into gj_invert_A values(1, 3, 3);
insert into gj_invert_A values(2, 1, 5);
insert into gj_invert_A values(2, 2, 7);
insert into gj_invert_A values(2, 3, 11);
insert into gj_invert_A values(3, 1, 13);
insert into gj_invert_A values(3, 2, 17);
insert into gj_invert_A values(3, 3, 19);
call gj_invert(3);
select * from gj_invert_B;
-- ans =
-- 
--   -2.250000   0.541667   0.041667
--    2.000000  -0.833333   0.166667
--   -0.250000   0.375000  -0.125000

drop table if exists gj_invert_A;
drop table if exists gj_invert_B;
