DROP DATABASE IF EXISTS riotdb;
CREATE DATABASE riotdb;
USE riotdb;

-- Table cleanup --
DROP TABLE IF EXISTS Metadata;
DROP TABLE IF EXISTS DB_Sources;
DROP TABLE IF EXISTS View_Reference;


-- Table creation --
CREATE TABLE DB_Sources (
    db_source_id    INT(6) UNSIGNED AUTO_INCREMENT,
    db_driver       VARCHAR(30) NOT NULL,
    db_name         VARCHAR(30) NOT NULL,
    username        VARCHAR(30),
    password        VARCHAR(64),
    next_vec_index  INT(10),
    PRIMARY KEY  (db_source_id),
    index (db_source_id)
) engine=myisam;


CREATE TABLE Metadata (
    metadata_id     INT(6)  UNSIGNED AUTO_INCREMENT,
    table_name      VARCHAR(40) NOT NULL,
    db_source_id    INT(6)  UNSIGNED ,
    size            VARCHAR(64),
    is_view         INT(2)  UNSIGNED DEFAULT 0,
    ref_counter     INT(6)  UNSIGNED DEFAULT 1,
    sxp_type        SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_obj         SMALLINT(2) UNSIGNED DEFAULT 1,
    sxp_named       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_gp          SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_mark        SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_debug       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_trace       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_spare       SMALLINT(2) UNSIGNED DEFAULT 1,
    sxp_gcgen       SMALLINT(2) UNSIGNED DEFAULT 0,
    sxp_gccls       SMALLINT(2) UNSIGNED DEFAULT 0,
    PRIMARY KEY     (metadata_id),
--    FOREIGN KEY     (db_source_id) REFERENCES DB_Sources ON DELETE CASCADE,
    INDEX           (metadata_id)
--    index (db_source_id)
) engine=myisam;


CREATE TABLE View_Reference (
    view_id         INT(6) UNSIGNED,
    table1_id       INT(6) UNSIGNED,
    table2_id       INT(6) UNSIGNED,
    PRIMARY KEY     (view_id),
    index           (view_id)
) engine=myisam;


-- Create Triggers --
DELIMITER |
CREATE TRIGGER increment_next_id AFTER INSERT ON Metadata
   FOR EACH ROW BEGIN
       UPDATE DB_Sources SET next_vec_index = next_vec_index + 1
       WHERE DB_Sources.db_source_id = NEW.db_source_id;
   END;
|

CREATE TRIGGER increment_ref_counter AFTER INSERT ON View_Reference
   FOR EACH ROW BEGIN
       UPDATE Metadata SET ref_counter = ref_counter + 1
       WHERE metadata_id = NEW.table1_id OR metadata_id = NEW.table2_id;
   END;
|

DELIMITER ;


-- Insert Local DB info --
INSERT INTO DB_Sources VALUE(NULL,'MySQL','riotdb','mysql',NULL,1);

-- Gaussian Elimination with Backward Substitution --

-- The system we are solving is Ax = b.  This procedure assumes the
-- following input/output views:
--
-- * Read-only input view named ge_solve_A(i, j, v) (with dimension m
-- by n);
--
-- * Read-only input view named ge_solve_b(i, v) (with dimension m);
--
-- * Output view named ge_solve_x(i, v) (with dimension n) for holding
-- the output vector x, originally empty.  This view must support
-- insert and delete-all.
--
-- If any division by zero is encountered, the procedure gives up and
-- deletes all elements from ge_solve_x.
--
-- This procedure also assumes that: *) All vectors/matrices are
-- dense; *) m <= n.  Finally, for holding temporary results, this
-- procedure assumes exclusive access to the tables as created below.

DROP TABLE IF EXISTS ge_solve_T; -- of dimension m by n+1 at runtime
CREATE TABLE ge_solve_T(i INTEGER, j INTEGER, v DOUBLE, PRIMARY KEY(i, j)) ENGINE = MYISAM;
DROP TABLE IF EXISTS ge_solve_vr; -- of dimension n at runtime
CREATE TABLE ge_solve_vr(j INTEGER PRIMARY KEY, v DOUBLE) ENGINE = MYISAM;
DROP TABLE IF EXISTS ge_solve_vc; -- of dimension m at runtime
CREATE TABLE ge_solve_vc(i INTEGER PRIMARY KEY, v DOUBLE) ENGINE = MYISAM;

DROP PROCEDURE IF EXISTS ge_solve;

DELIMITER //
CREATE PROCEDURE ge_solve
(IN m INTEGER, IN n INTEGER)
ge_solve_LABEL:
BEGIN
  DECLARE k INT;
  DECLARE z DOUBLE;

  -- Set up the augmented matrix.

  DELETE FROM ge_solve_T;
  INSERT INTO ge_solve_T SELECT * FROM ge_solve_A;
  INSERT INTO ge_solve_T SELECT i, n+1, v FROM ge_solve_b;

  -- Transform into upper triangluar form.

  SET k = 1;
  WHILE k <= m DO
    -- Check if we will be dividing by zero here:
    SELECT v INTO z FROM ge_solve_T WHERE i = k AND j = k;
    IF ABS(z) < 1e-10 THEN
      DELETE FROM ge_solve_x;
      LEAVE ge_solve_LABEL;
    END IF;

    -- Reduce the rows k+1 through m using the k-th row.

    IF k < m THEN
      -- First, get the partial k-th row (columns after the k-th):
      DELETE FROM ge_solve_vr;
      INSERT INTO ge_solve_vr SELECT j, v FROM ge_solve_T WHERE i = k AND j > k;

      -- Second, looking at the k-th column, compute the multipliers to
      -- be used for reducing the corresponding rows (k+1 through m):
      DELETE FROM ge_solve_vc;
      INSERT INTO ge_solve_vc SELECT i, v/z FROM ge_solve_T WHERE i > k AND j = k;

      -- Update the rows (we skip updates to below-diagonal entries):
      UPDATE ge_solve_T T
        SET v = v -
          (SELECT v FROM ge_solve_vc WHERE i = T.i) *
          (SELECT v FROM ge_solve_vr WHERE j = T.j)
        WHERE i > k AND j > k;
    END IF;

    SET k = k+1;

  END WHILE;

  -- Backward substitution.

  SET k = n;

  WHILE k > m DO
    -- Underdetermined system:
    INSERT INTO ge_solve_x
    VALUES(k, 0);
    SET k = k-1;
  END WHILE;

  INSERT INTO ge_solve_x
    VALUES(m, (SELECT v FROM ge_solve_T WHERE i = m AND j = n+1) /
              (SELECT v FROM ge_solve_T WHERE i = m AND j = m));
  SET k = k-1;

  WHILE k > 0 DO

    SELECT SUM(T.v * x.v) INTO z
    FROM ge_solve_T T, ge_solve_x x
    WHERE T.i = k AND T.j = x.i AND x.i > k;

    INSERT INTO ge_solve_x
      VALUES(k, (SELECT v - z FROM ge_solve_T WHERE i = k AND j = n+1) /
                (SELECT v FROM ge_solve_T WHERE i = k AND j = k));

    SET k = k-1;
  END WHILE;

END//
DELIMITER ;

-- Gauss-Jordan Elimination --

-- This procedure assumes the following input/output views:
--
-- * Read-only input view named gj_invert_A(i, j, v) (with dimension n
-- by n);
--
-- * Output view named gj_invert_B(i, j, v) (with dimension n by n)
-- for holding the inverted matrix, originally empty.  This view must
-- support insert and update.
--
-- If any division by zero is encountered, the procedure gives up and
-- leaves gj_invert_B empty.
--
-- This procedure also assumes that: *) All vectors/matrices are
-- dense.  Finally, for holding temporary results, this procedure
-- assumes exclusive access to the tables as created below.

DROP TABLE IF EXISTS gj_invert_T; -- of dimension n by 2n at runtime
CREATE TABLE gj_invert_T(i INTEGER, j INTEGER, v DOUBLE, PRIMARY KEY(i, j)) ENGINE = MYISAM;
DROP TABLE IF EXISTS gj_invert_vr; -- of dimension 2n at runtime
CREATE TABLE gj_invert_vr(j INTEGER PRIMARY KEY, v DOUBLE) ENGINE = MYISAM;
DROP TABLE IF EXISTS gj_invert_vc; -- of dimension n at runtime
CREATE TABLE gj_invert_vc(i INTEGER PRIMARY KEY, v DOUBLE) ENGINE = MYISAM;

DROP PROCEDURE IF EXISTS gj_invert;

DELIMITER //
CREATE PROCEDURE gj_invert
(IN n INTEGER)
gj_invert_LABEL:
BEGIN
  DECLARE k INT;
  DECLARE z DOUBLE;

  -- The augmented matrix is made up of gj_invert_A and the identity
  -- matrix to begin with.

  DELETE FROM gj_invert_T;
  INSERT INTO gj_invert_T SELECT * FROM gj_invert_A;
  INSERT INTO gj_invert_T SELECT i, j+n, IF(i=j, 1, 0) FROM gj_invert_A;

  -- Transform into row echelon form.

  SET k = 1;
  WHILE k <= n DO
    -- Reduce the rows k+1 through n using the k-th row.

    -- Check if we will be dividing by zero here:
    SELECT v INTO z FROM gj_invert_T WHERE i = k AND j = k;
    IF ABS(z) < 1e-10 THEN
      LEAVE gj_invert_LABEL;
    END IF;

    -- First, scale the k-th row (ignoring columns before the k-th):
    UPDATE gj_invert_T SET v = v/z WHERE i = k AND j >= k;

    IF k < n THEN
      -- Remember a copy of the scaled row (columns after the k-th) for convenience:
      DELETE FROM gj_invert_vr;
      INSERT INTO gj_invert_vr SELECT j, v FROM gj_invert_T WHERE i = k AND j > k;

      -- Second, looking at the k-th column, remember the multipliers to
      -- be used for reducing the corresponding rows (k+1 through n):
      DELETE FROM gj_invert_vc;
      INSERT INTO gj_invert_vc SELECT i, v FROM gj_invert_T WHERE i > k AND j = k;

      -- Update the rows (we skip updates to below-diagonal entries):
      UPDATE gj_invert_T T
        SET v = v -
          (SELECT v FROM gj_invert_vc WHERE i = T.i) *
          (SELECT v FROM gj_invert_vr WHERE j = T.j)
        WHERE i > k AND j > k;
    END IF;

    SET k = k+1;

  END WHILE;

  -- Going backwards to transform into reduced row echolon form.  We
  -- will now copy the right half of the augmented matrix out into
  -- gj_invert_B, and we can update it and ignore gj_invert_T.

  INSERT INTO gj_invert_B
    SELECT i, j-n, v FROM gj_invert_T WHERE j > n;

  SET k = n;
  WHILE k > 1 DO

    -- First, remember the k-th row:
    DELETE FROM gj_invert_vr;
    INSERT INTO gj_invert_vr SELECT j, v FROM gj_invert_B WHERE i = k;

    -- Update the rows:
    UPDATE gj_invert_B T
      SET v = v -
        (SELECT v FROM gj_invert_T WHERE i = T.i AND j = k) *
        (SELECT v FROM gj_invert_vr WHERE j = T.j)
      WHERE i < k;

    SET k = k-1;

  END WHILE;


END//
DELIMITER ;
