Step 2.1: HNSW Algorithm Encapsulation from the implementation plan.   
  Here's what was created:                                                                
                                                                                          
  Files Created                                                                 
                                                                                          
  1. core/src/main/scala/com/company/ann/core/index/VectorItem.scala                      
    - VectorItem class implementing hnswlib-core's Item interface                         
    - Wraps Long ID and Array[Float] vector                                               
  2. core/src/main/scala/com/company/ann/core/index/HNSWLibIndex.scala                    
    - HNSWLibIndex class implementing the HNSWIndex trait                                 
    - Supports both Euclidean and Cosine distance functions                               
    - add() / addAll() for inserting vectors                                              
    - search() with configurable ef parameter                                             
    - save() / load() for persistence with metadata                                       
    - contains(), remove(), getItems() utilities                                          
    - IndexMetadata case class for serialization                                          
  3. core/src/test/scala/com/company/ann/core/index/HNSWIndexTest.scala                   
    - 20 comprehensive tests covering:                                                    
        - Basic CRUD operations                                                           
      - Recall verification (100% achieved!)                                              
      - Distance function correctness                                                     
      - Persistence and loading                                                           
      - Edge cases and error handling                                                     
      - Performance benchmarks                                                            
                                                                                          
  Test Results                                                                            
                                                                                          
  All 37 core tests passed:                                                               
  - Recall: 100% on clustered and random data (target was >90%)                           
  - Speedup: 68x faster than brute force                                                  
  - Batch add: 5x faster than single adds 