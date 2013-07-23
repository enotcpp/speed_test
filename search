#pragma once

#include "stdafx.h"

#include <stdio.h>      /* printf, scanf, NULL */
#include <stdlib.h>     /* calloc, exit, free */
#include <ctime>       /* clock_t, clock, CLOCKS_PER_SEC */

#include <amp.h>
#include <ppl.h>
#include <concurrent_vector.h>

#include <array>
#include <numeric>

#include <windows.h>

using namespace concurrency;


namespace test
{

const size_t c_array_size = 10000000;

/* Fields */
enum T_field_enum { amount_of_money_e, gender_e, age_e, code_e, height_e,   /*<<<<<- add fields here */	last_e };

/* Row */
struct T_cash_account_row_bit 
{
    unsigned code:20;			// 0 - 1000000
	unsigned gender:1;			// 0 - 1
	unsigned age:7;			    // 0 - 100	
	unsigned amount_of_money:20;// 0 - 1000000
	unsigned height:9;			// 0 – 300
};

struct T_cash_account_row 
{
    unsigned code;			// 0 - 1000000
	unsigned gender;			// 0 - 1
	unsigned age;			    // 0 - 100	
	unsigned amount_of_money;// 0 - 1000000
	unsigned height;			// 0 – 300
};

/* ----------------------------------------------------------------------- */

/* Generate random data for the one row */
static inline struct T_cash_account_row generate_row() 
{
	struct T_cash_account_row cash_account_row;
	cash_account_row.age = rand() % 100;
	cash_account_row.amount_of_money = (rand() % 1000)*(rand() % 1000);
	cash_account_row.code = (rand() % 1000)*(rand() % 1000);
	cash_account_row.gender = rand() % 2;
	cash_account_row.height = rand() % 300;
	return cash_account_row;
}
/* ----------------------------------------------------------------------- */

/* Filters */
struct T_range_filters 
{
    struct T_cash_account_row begin, end;
    /* bytes array or bitset from https://gist.github.com/jmbr/667605 */
    unsigned char use_filter[last_e]; 
};

/* ----------------------------------------------------------------------- */

/* Compare row with filters */
static inline unsigned char test_predicate 
(
	struct T_cash_account_row const*const	row, 
    struct T_range_filters const*const		range_filters
) 
{    
    return 
        (!range_filters->use_filter[amount_of_money_e] || 
            (row->amount_of_money >= range_filters->begin.amount_of_money &&
            row->amount_of_money <= range_filters->end.amount_of_money)) &&
        (!range_filters->use_filter[gender_e] || 
            (row->gender >= range_filters->begin.gender && row->gender <= range_filters->end.gender)) &&
        (!range_filters->use_filter[age_e] || 
            (row->age >= range_filters->begin.age && row->age <= range_filters->end.age)) &&
        (!range_filters->use_filter[code_e] || 
            (row->code >= range_filters->begin.code && row->code <= range_filters->end.code)) &&
        (!range_filters->use_filter[height_e] || 
            (row->height >= range_filters->begin.height && row->height <= range_filters->end.height));
}
/* ----------------------------------------------------------------------- */

/* ----------------------------------------------------------------------- */
// C
/* ----------------------------------------------------------------------- */

static inline size_t search(struct T_cash_account_row const*const array_ptr, const size_t c_array_size,
	struct T_cash_account_row *const result_ptr, struct T_range_filters const*const range_filters) 
{
	size_t result_size = 0;
	size_t i; /* loop index */
	for(i = 0; i < c_array_size; ++i) 
	{
		if(test_predicate(array_ptr + i, range_filters)) 
			result_ptr[result_size] = array_ptr[i], ++result_size;
	}
	return result_size; 
}

/* ----------------------------------------------------------------------- */
// PPL
/* ----------------------------------------------------------------------- */

static inline size_t parallel_search(struct T_cash_account_row const*const array_ptr, const size_t c_array_size,
	struct T_cash_account_row *const result_ptr, struct T_range_filters const*const range_filters) 
{	
	size_t result_size = 0;
	concurrency::parallel_for((size_t)0, c_array_size, [&](size_t i)
	{
		if(test_predicate(array_ptr + i, range_filters)) InterlockedIncrement(&result_size);
	});
	return result_size;	
}

/* ----------------------------------------------------------------------- */
// AMP
/* ----------------------------------------------------------------------- */

void amp_search
(
	struct T_cash_account_row	*	row,
	struct T_range_filters		*	range_filters,
	int								size
)
{
	unsigned *res = new unsigned[size];

	//+------------------------------------------------------------------------+
	// Инициализация фильтра:
	//+------------------------------------------------------------------------+

	T_cash_account_row min_range	= range_filters->begin;
	T_cash_account_row max_range	= range_filters->end;
	unsigned use_age				= range_filters->use_filter[age_e];		
	unsigned use_amount_of_money	= range_filters->use_filter[amount_of_money_e];
	unsigned use_code				= range_filters->use_filter[code_e];		
	unsigned use_gender				= range_filters->use_filter[gender_e];
	unsigned use_height				= range_filters->use_filter[height_e];
		
	concurrency::array_view<const T_cash_account_row, 1>	cash(size, row);
	concurrency::array_view<unsigned, 1>					result(size, res);

	size_t result_size = 0;
	clock_t end, start;
	const int tile_size = 1000;
	printf ("AMP-Searching...\n");
	
	start = clock();
	parallel_for_each(concurrency::extent<1>(size).tile<tile_size>(), 
	[=](tiled_index<tile_size> idx) restrict(amp)
	{
		const T_cash_account_row& t = cash[idx.local];

		tile_static unsigned filter[last_e];
		tile_static unsigned range[last_e]; 

		filter[amount_of_money_e]	= use_amount_of_money;
		filter[gender_e]			= use_gender;
		filter[age_e]				= use_age;
		filter[code_e]				= use_code;
		filter[height_e]			= use_height;

		range[amount_of_money_e]	= max_range.amount_of_money - min_range.amount_of_money; 
		range[gender_e]				= max_range.gender			- min_range.gender;
		range[age_e]				= max_range.age				- min_range.age;
		range[code_e]				= max_range.code			- min_range.code;
		range[height_e]				= max_range.height			- min_range.height;
		
		if(!filter[amount_of_money_e]		|| (range[amount_of_money_e] >= t.amount_of_money))
			if((!filter[gender_e]			|| (range[gender_e]	>= t.gender	)))
				if(!filter[code_e]			|| (range[code_e]	>= t.code	)) 
					if(!filter[height_e]	|| (range[height_e]	>= t.height	))
						if(!filter[age_e]	|| (range[age_e]	>= t.age	))
							result[idx.global] = 1;
	});
	//result.synchronize();	// Если не выполняется больше работы.
	result_size = std::count(&result[0], &result[size], 1);
	//result_size = std::accumulate(&result[0], &result[size], 0);
	end = clock();
	float c_took_time = (float)(end - start);
	printf ("AMP-search took %f seconds.\n", c_took_time/CLOCKS_PER_SEC);
	printf ("Found rows: %d \n", result_size);

	delete [] res;
}

int begin_search_test() 
{
	size_t i; /* loop index */
    struct T_cash_account_row *const array_ptr = 
		( struct T_cash_account_row *)calloc(c_array_size, sizeof(struct T_cash_account_row));
    if(array_ptr == NULL) 
	{
		printf ("calloc error\n");
		exit(1);
	}

	/* initialize random seed: */
	/* srand (time(NULL)); */

	/* Fill table random data */
	for(i = 0; i < c_array_size; ++i) array_ptr[i] = generate_row();
	printf ("Generated rows: %d \n", c_array_size);

    struct T_range_filters range_filters = {};

	range_filters.use_filter[amount_of_money_e] = rand()%1 + 0;
	range_filters.use_filter[gender_e] = rand()%1 + 0;
	range_filters.use_filter[height_e] = rand()%1 + 0;

    range_filters.begin.age = rand() % 100;
    range_filters.end.age = range_filters.begin.age + 5;
    range_filters.use_filter[age_e] = rand()%1 + 1;

    range_filters.begin.code = rand() % 30000;
    range_filters.end.code = range_filters.begin.code + 5;
    range_filters.use_filter[code_e] = rand()%1 + 1;
    
    struct T_cash_account_row *const result_ptr = ( struct T_cash_account_row *)calloc(c_array_size, sizeof(struct T_cash_account_row));

	size_t result_size;
	clock_t end, start;

	printf ("C-Searching...\n");

	start = clock();
	result_size = search(array_ptr, c_array_size, result_ptr, &range_filters);
	end = clock();
	float c_took_time = (float)(end - start);
	printf ("C-search took %f seconds.\n", c_took_time/CLOCKS_PER_SEC);

	printf ("Found rows: %d \n", result_size);
    

	//+------------------------------------------------------------------------+
	//| amp
	//+------------------------------------------------------------------------+

	amp_search(array_ptr, &range_filters, c_array_size);

	//+------------------------------------------------------------------------+
	//| ppl
	//+------------------------------------------------------------------------+

	printf ("PPL-Searching...\n");

	start = clock();
	result_size = parallel_search(array_ptr, c_array_size, result_ptr, &range_filters);
	end = clock();
	c_took_time = (float)(end - start);
	printf ("PPL-search took %f seconds.\n", c_took_time/CLOCKS_PER_SEC);

	printf ("Found rows: %d \n", result_size);
}

//+------------------------------------------------------------------------+
}		// namespace test
