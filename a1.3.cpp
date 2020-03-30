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
			

			// good
			aggs_gb_C[r_C.C][0] += 1
			aggs_gb_C[r_C.C][1] += r_A.A
			aggs_gb_C[r_C.C][2] += r_B.B
			

			par_aggs[6] += r_B.B * r_C.C
		}

		aggs_gb_B[r_B.B][0] += par_aggs_gb_B[0]			// sum(1) group by B
		aggs_gb_B[r_B.B][1] += par_aggs_gb_B[1]			// sum(A) group by B
		aggs_gb_B[r_B.B][2] += par_aggs_gb_B[2]			// sum(C) group by B
	}

//good
	aggs_gb_A[r_A.A][0] += par_aggs_gb_A[0]
	aggs_gb_A[r_A.A][1] += par_aggs_gb_A[1]
	aggs_gb_A[r_A.A][2] += par_aggs_gb_A[2]
	aggs[0] += par_aggs_gb_A[0]
	aggs[1] += par_aggs_gb_A[0] * r_A.A
	aggs[2] += par_aggs_gb_A[1]							//SUM(B)
	aggs[3] += par_aggs_gb_A[2]							//SUM(C)
	aggs[4] += par_aggs_gb_A[1] * r_A.A
	aggs[5] += par_aggs_gb_A[2] * r_A.A
	aggs[6] += par_aggs[6]
}



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
			

			// good
			aggs_gb_C[r_C.C][0] += 1
			aggs_gb_C[r_C.C][1] += r_A.A
			aggs_gb_C[r_C.C][2] += r_B.B
			
			par_aggs[6] += r_B.B * r_C.C
		}

		aggs_gb_B[r_B.B][0] += par_aggs_gb_B[0]			// sum(1) group by B
		aggs_gb_B[r_B.B][1] += par_aggs_gb_B[1]			// sum(A) group by B
		aggs_gb_B[r_B.B][2] += par_aggs_gb_B[2]			// sum(C) group by B
	}

//good
	aggs_gb_A[r_A.A][0] += par_aggs_gb_A[0]
	aggs_gb_A[r_A.A][1] += par_aggs_gb_A[1]
	aggs_gb_A[r_A.A][2] += par_aggs_gb_A[2]
	aggs[0] += par_aggs_gb_A[0]
	aggs[1] += par_aggs_gb_A[0] * r_A.A
	aggs[2] += par_aggs_gb_A[1]							//SUM(B)
	aggs[3] += par_aggs_gb_A[2]							//SUM(C)
	aggs[4] += par_aggs_gb_A[1] * r_A.A
	aggs[5] += par_aggs_gb_A[2] * r_A.A
	aggs[6] += par_aggs[6]
}
