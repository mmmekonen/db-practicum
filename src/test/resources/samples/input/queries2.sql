SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G ORDER BY Sailors.A;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D ORDER BY Sailors.A;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND Sailors.B = 150 ORDER BY Sailors.A;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A = S2.A ORDER BY Sailors.A;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT * FROM Sailors S1, Sailors S2 ORDER BY Sailors.A;
SELECT * FROM Sailors, Reserves ORDER BY Sailors.A;