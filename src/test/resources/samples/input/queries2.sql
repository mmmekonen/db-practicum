SELECT * FROM Sailors WHERE Sailors.A >= 4;
SELECT * FROM Sailors WHERE Sailors.B < Sailors.C;
SELECT Sailors.A, Sailors.B FROM Sailors WHERE Sailors.A >= 4;
SELECT Sailors.A, Sailors.B FROM Sailors;
SELECT Sailors.C, Sailors.B, Sailors.A FROM Sailors WHERE Sailors.A >= 0;
SELECT * FROM Sailors S WHERE S.A >= 4;
SELECT S.A, S.B FROM Sailors S WHERE S.A >= 4;
SELECT S.A, S.B FROM Sailors S;
SELECT S.C, S.B, S.A FROM Sailors S WHERE S.A >= 0;
SELECT DISTINCT Reserves.G FROM Reserves;
SELECT * FROM Sailors S ORDER BY S.B;
SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G;
SELECT DISTINCT Sailors.A, Reserves.G, Boats.D, Sailors.C FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Boats.D > Sailors.C;