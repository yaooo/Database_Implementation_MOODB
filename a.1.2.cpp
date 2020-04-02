// init aggs_gb_A & aggs_gb_B && aggs_gb_C && aggs
for (r_A <- R_TRIE){	

	// init par_aggs_gb_A  & par_aggs
	
	for(r_B <- r_A.LIST){

		// init par_aggs_gb_B

		for(r_C <- r_B.LIST){
			
			par_aggs_gb_A[0] += 1
			par_aggs_gb_A[1] += r_B.B
			par_aggs_gb_A[2] += r_C.C


			par_aggs_gb_B[0] += 1
			par_aggs_gb_B[1] += r_A.A
			par_aggs_gb_B[2] += r_C.C
			

			aggs_gb_C[r_C.C][0] += 1
			aggs_gb_C[r_C.C][1] += r_A.A
			aggs_gb_C[r_C.C][2] += r_B.B
			

			par_aggs[0] += 1
			par_aggs[1] += 1
			par_aggs[2] += r_B.B
			par_aggs[3] += r_C.C
			par_aggs[4] += r_B.B
			par_aggs[5] += r_C.C
			par_aggs[6] += r_B.B * r_C.C
		}
		aggs_gb_B[r_B.B][0] += par_aggs_gb_B[0]	
		aggs_gb_B[r_B.B][1] += par_aggs_gb_B[1]	
		aggs_gb_B[r_B.B][2] += par_aggs_gb_B[2]	
	}

	aggs_gb_A[r_A.A][0] += par_aggs_gb_A[0]
	aggs_gb_A[r_A.A][1] += par_aggs_gb_A[1]
	aggs_gb_A[r_A.A][2] += par_aggs_gb_A[2]

	aggs[0] += par_aggs[0]
	aggs[1] += par_aggs[1] * r_A.A
	aggs[2] += par_aggs[2]
	aggs[3] += par_aggs[3]
	aggs[4] += par_aggs[4] * r_A.A
	aggs[5] += par_aggs[5] * r_A.A
	aggs[6] += par_aggs[6]
}



// parse all SQL aggregate queries and store them in a query list

evaluate(){
    // init array with its size = Trie.depth
    traverse(Trie.root, 0, array);
    // print out the computational results for each query
}

traverse(root, level, array){
    if (reach the leaf nodes of the trie){
        for(query <- querie_list){
        	
        	calculatePartial(q, array)
        	// This function calculates partial aggregates for all attributes from the queries,
        	// except the most inner attribute of the schema


        	if(query.groupByFiedl == schema.lastAttribute){
	        	// directly calculate the aggs_gb_(most_inner_attr) based on the schema, 
	        	// and input queris		
        	}        
        }
        return;
    }
    map <- root.children
    for(key : map.keySet){
        Trie.TrieNode next = map.get(key);
        if(next != null){
            array[level] = key;
            traverse1(next, level + 1, str);

            for(query <- query_list){
            	if(raech the corrsponding attr in the trie){
            		calculate(query, array, level)
            	}	
            }
            
        }
    }
}


calculatePartial(query, array){

    if(this query is to calculate the aggs_gb_(most_inner_attr)) 
    	return

    type = query.type
    // GROUPBYQUERY = 1; GENERALQUERY = 0;

    if(type == Query.GROUPBYQUERY){
    	// parse the query and tokenize it 
    	index = 0
        for(op <- query_token_list) {

        	if(!op.contains("SUM")){
                query.par_aggs[index] = key;
            }
            else if (op.equals("SUM(1)")) {
                query.par_aggs[index] += 1;

            } else if (op.contains("SUM")) { 
            	// calculate the value of this "expr", store it in the variable "sum_expr"
            	query.par_aggs[index] += sum_expr
            }
            index += 1
        }
    }
    else if(type == Query.GENERALQUERY){

		// parse the query and tokenize it 
    	index = 0
        for(op <- query_token_list) {
			if (op.equals("SUM(1)")) {
                query.par_aggs[index] += 1;

            } else if (op.contains("SUM")) {
                // calculate the value of this "expr", store it in the variable "sum_expr"
            	query.par_aggs[index] += sum_expr
            }
            index += 1
		}
    }
}
