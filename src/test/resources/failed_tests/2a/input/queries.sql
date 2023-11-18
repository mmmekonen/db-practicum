SELECT * FROM Sailors ORDER BY Sailors.B;
SELECT Boats.F, Boats.D FROM Boats ORDER BY Boats.D;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G ORDER BY Sailors.C;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G ORDER BY Sailors.C, Reserves.G;
SELECT DISTINCT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G ORDER BY Sailors.C;