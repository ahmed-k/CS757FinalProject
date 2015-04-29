# CS757FinalProject
CS757: Mining Massive Datasets Final Project


Things To Do:


1. We need to determine the distance measure used to compare high-dimensional points.
2. We need to figure out how to init clusters, there are 2 approaches outlined in page 242 of the book.
3. Pick Right Value For k:multiples of five, or 10 (book suggests 1,2,4,8...)  
3. BFR on clusters 
4. Analyze data (probably using user_attr found in dataset) 
NICE TO HAVE: reduce dimensions. 



#1: 
Once we have found centroids. We can use Mahalanobis distance to compare points to the centroid.
Prior, we need another metric (Normalized Cosine Distance?) 
We can both work on this in parallel. 


#2: 
Ahmed will do number 1 (pick points as far away from each other as possible)
Aaron will do number 2 (cluster a sample) 



