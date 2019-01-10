# Jaccard-similarity

this project use hadoop to calculate jaccard similarity between statements

## Task

- **Segmentation of a statement**

Each sentence is divided into three characters regardless the speace、symbol such as _ , -  etc...

Each letter retains the original capitalization

```
today is Monday
["tod","oda","day","ay_","y_i","_is","is_","s_M","_Mo","Mon","ond","nda","day"]
```

- **First Map Stage**

Input format:

```
ID	words
1   Jurgen Annevelink
```

Output format

```
key: prefix_item1 value:ID|item1#item2#item3  ...
key: prefix_item2 value:ID|item1#item2#item3  ...
key: prefix_item3 value:ID|item1#item2#item3  ...
key: Jur value:1|Jur#urg#rge#gen#en #n A#Ann#nne  ...
```

- **First Reduce Stage**

This stage calculate the similarity with same prefix item

Input format:

```
key: prefix_item1
value: ID1|item1,item2,item3,... ; ID2|item1,item2,item3,...  ; ID3|item1,item2,item3,...  ; ...
```

Output format:

```
ID1 	ID2 	similarity
ID1 	ID3 	similarity
ID2 	ID2 	similarity
```

- **Final MapReduce Stage**

Remove the duplicates

## Deployment

- JDK1.8
- Hadoop 2.9.0
- Maven
- IntelliJ IDEA



## How to use

using `hdfs` to uoload test data into dirctory input

`hadoop jar jaccard.jar Jaccard input output`

## Simple

> input

```
1 Jurgen Annevelink
2 Rafiul Ahad
3 Amelia Carlson
4 Daniel H. Fishman
5 Michael L. Heytens
6 William Kent
7 Jos
8 Yuri Breitbart
9 Hector Garcia-Molina
10 Abraham Silberschatz
11 Stavros Christodoulakis
12 Leonidas Koveos
13 Umeshwar Dayal
......
......
```

> output

```
(1,90)		1.0
(4,89)		1.0
(5,385)		0.3
(5,551)		0.3333333333333333
(5,1045)	0.3125
(5,1789)	0.30434782608695654
(5,2746)	0.3157894736842105
(6,22)		0.5714285714285714
(6,39)		0.5714285714285714
(6,59)		1.0
(6,62)		1.0
(6,97)		1.0
(6,270)		0.375
(6,495)		0.3157894736842105
(6,790)		0.3
......
......
```

