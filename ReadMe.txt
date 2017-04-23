------------------------------------------------------------------
個別數據產生的output檔案放在各個資料夾裡
C1_EU = C1.txt Euclidean
C2_EU = C2.txt Euclidean
C1_MA = C1.txt Manhattan
C2_MA = C2.txt Manhattan
裡面分別有三種不同的output
between_dist 是center相互之間的距離
center 是20iter後的center
outCost 是20iter個別的Cost
--------------------------------------------------------------------
CODE放在主資料夾底下
分別為
MDA_HW3_Euclidean.java
MDA_HW3_Manhattan.java

這裡多寫了一個kmean.sh檔案
用來清除下一次使用前，必須清除上一次產生的資料
---------------------------------------------------------------------