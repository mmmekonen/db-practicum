SELECT * FROM Sailors WHERE Sailors.A > 4000 ORDER BY Sailors.A;
SELECT * FROM Sailors S WHERE 4000 < S.A AND S.C <= 2000 ORDER BY S.A;
SELECT * FROM Sailors S WHERE S.C <= 2000 ORDER BY S.A;
SELECT * FROM Boats B WHERE B.E > 3000 AND 7000 >= B.E ORDER BY B.D;
SELECT * FROM Boats B WHERE B.E = 330 AND B.E < 8000 ORDER BY B.D;
SELECT * FROM Sailors S, Boats B WHERE B.D = S.C AND B.E > 6000 AND S.A < 4000 ORDER BY S.A;