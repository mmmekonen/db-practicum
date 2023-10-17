SELECT * FROM Test1 T1, Test1 T2 WHERE T1.A = T2.B;
SELECT * FROM Test1, Test2 WHERE Test1.A = Test2.C;
SELECT * FROM Test1, Test2, Test3 WHERE Test1.A = Test2.B AND Test2.A = Test3.C;