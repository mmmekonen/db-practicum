SELECT * FROM Test1 T1, Test1 T2 WHERE T1.A = T2.B AND T2.A < 50;
SELECT * FROM Test1, Test2 WHERE Test1.A = Test2.C AND Test2.B >= 100;
SELECT * FROM Test1, Test2, Test3 WHERE Test1.A = Test2.B AND Test2.A = Test3.C AND Test2.B >= 100 AND Test1.A < 120;