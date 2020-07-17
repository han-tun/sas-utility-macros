/******************************************************************************\
* Name: casload
*
* Author: Stu Sztukowski, SAS(r) US Professional Services
*
* Purpose: A single-use program to automate multiple steps associated with CAS load, unload, append, promote, delete, 
*		   and save tasks from SAS 9 or SPRE environments where the user does not have direct CASLIB access. 
*		   This tool improves CAS table efficiency using varchars and standardizes variable case. 
*		   Multiple datasets and options can be mixed and matched. Some features include:
*
*		- Automatically convert chars into varchars; users can control the minimum required bytes for conversion
*		- Load, promote, append, save, and delete multiple datasets in a single step
*		- Automatically save partitions and row order when saving data
*		- Fast Promote method: Load into a temporary CASLIB, delete old data, then promote new data – this method allows for near-seamless VA updates, but will temporarily double the amount of data used
*		- Automatically save datasets to CAS for permanent data storage when promoting
*		- Support for output dataset options
*		- Automatically delete backing store when deleting datasets to prevent data remnants from continuing to take up disk space
*		- Support to standardize all var names to lowercase
*		- Automatically remove labels from data
*
*		The intended audience of this program is a user who:
*			- Loads data from SPRE or SAS 9 into a CASLIB
*			- Loads data from a separate environment and saving it to a PATH, DNFS, or HDFS
*			- Would like to standardize variable case and labels for VA
*			- Would like to convert variables to varchars
*
*		Users who can load data directly from a CASLIB with data connectors should load data using PROC CASUTIL with the 
*		CASDATA statement.
*
*		More information about loading data with the CASDATA statement can be found below in "Form 1:"
*		https://go.documentation.sas.com/?docsetId=casref&docsetTarget=n03spmi9ixzq5pn11lneipfwyu8b.htm&docsetVersion=3.5&locale=en
*
*
* Parameters: data	  	     | Datasets to load or append in CAS. Multiple datasets are allowed. Separate datasets with spaces. Input dataset options are not supported.
*			  casout  	     | Optional. One or two-level output CAS dataset names that correspond with datasets specified in the DATA option.
*							   Supports output CAS dataset options. If one-level dataset names are specified with the CASLIB= option, CASLIB will apply to
*							   these datasets. Separate multiple datasets with spaces.
*							   If DELETE=YES: All datasets specified in CASOUT will be deleted.
*			  casdata 	     | Alias for casout.
*			  caslib  	     | Optional. Apply all operations to datasets in CASOUT. If one-level dataset names are specified with the CASOUT= option, all
*							   operations will apply to those datasets in CASLIB.
*							   Default: CASUSER
*			  lib		     | Optional. Read all datasets in LIB. If one-level dataset names are specified with the DATA= option, all one-level
*							   dataset names will default to LIB.
*							   Default: WORK
*			  promote 	     | Optional. Promote all datasets unless overridden with output dataset options. Default: NO
*			  save	  	     | Optional. If PROMOTE=YES: Save all datasets to each dataset's CASLIB in .sashdat format. Default: YES
*			  delete  	   	 | Optional. Delete all datasets in CASOUT. WARNING: If deletesource=YES (default), the source data will be deleted.
*							   Default: NO
*			  deletesource 	 | Optional. If DELETE=YES, delete the associated .sashdat file for casout dataset. 
*							   Default: YES.
*							   WARNING: This will completely remove a backing store. Be sure this is not the only source you have!
*			  append	   	 | Optional. Append a SAS dataset to an existing CAS table. Valid values: YES, NO, FORCE. Default: NO
*			  label	   	   	 | Optional. Keep all variable labels. Default: NO
*			  lowcase	   	 | Optional. Standardize all variables as lower-case. Default: NO
*			  fastpromote	 | Optional. Load data to a temporary CASLIB, remove the old table, then promote the new table. 
*							   This will significantly reduce VA report downtime, but will temporarily double the amount of memory usage.
*							   Default: YES
*			  varchar	   	 | Convert all character variables > [minvarcharlen] bytes in length to Varchar. Default: YES	  
*			  minvarcharlen  | Minimum bytes to convert characters to varchars. Default: 16
*
* Dependencies/Assumptions: 
* 	1. A CAS session is established
*	2. All required CASLIBs are assigned
*	
* Examples:
	1. Load multiple CAS tables to a local CASUSER session

		%casload(data=sashelp.cars sashelp.air sashelp.pricedata, caslib=CASUSER);

	2. Load all datasets in a library to a local CASUSER session
		%casload(lib=work, caslib=CASUSER);

	3. Promote multiple CAS tables, apply partitions and orderby, and save them permanently

		%casload(data=sashelp.cars
							sashelp.pricedata
					 , casout=casuser.cars(partition=(make) orderby=(model) )
							  casuser.pricedata(partition=(regionname) orderby=(date) )
					 , promote=yes
					  );

	4. Append sashelp.cars to an existing CAS table without VARCHAR formats

			data casuser.cars2;
				set sashelp.cars;
			run;

			%casload(data=sashelp.cars
						 , casout=casuser.cars2
						 , append=FORCE
						 , varchar=NO
						  );

	5. Delete a dataset in a CASLIB and remove its .sashdat file:

			%casload(casout=casuser.cars, delete=yes); 
		OR: %casload(casdata=casuser.cars, delete=yes);

	6. Unload a table from memory but do not delete its .sashdat file:
			%casload(casout=casuser.cars, delete=yes, deletesource=NO);
		OR:	%casload(casdata=casuser.cars, delete=yes, deletesource=NO);

	7. Apply multiple operations within a single statement:
		a. Load and partition a CAS table
		b. Append a CAS table
		c. Promote a CAS table

			%casload(data=sashelp.cars sashelp.air sashelp.pricedata
						 , casout=casuser.cars casuser.air(append=YES) casuser.pricedata(promote=YES)
						  );
*	
*		 
* History: 07JAN2020 Stu | v0.1 - Initial beta release
*		   08JAN2020 Stu | v0.2 - Add option to load an entire library
*		   28JAN2020 Stu | v0.3 - Fixed a bug that could cause datasets to not be deleted
*								   - Moved default lib and caslib to be after error checking statements
*								   - Added checks if a CASLIB exists before loading and warns the user if it does not
*								   - Added checks if a dataset exists before deleting and warns the user if it does not
*								   - Added checks if a library is already empty before deleting and warns the user if it is
*		   14FEB2020 Stu | v0.4 - Bug fixes
*									- Resolved data being loaded to CASUSER if user does not specify the caslib= option
*									- Resolved bug where casload would error out if casout options were specified
*									- A note is no longer made in the log when unpromoted data is loaded to CAS but
*									  save=YES
*		   01JUN2020 Stu | v0.5 - Added fastpromote option and set it as default to yes. First loads to a temp CASLIB, 
*									 then promotes the table. This drastically reduces VA report downtime.
*		   14JUL2020 Stu | v0.6 - Bug fixes.
*								   - A temporary CASLIB named STAGECAS is made to load data in order to allow users to
*									 load and promote to CASUSER if FASTPROMOTE=YES.
*								   - Users can now specify a minimum varchar conversion length with minvarcharlen=.
*								   - When saving, added notes that partitions and row order will be saved.
*								   - Disabled the ability to delete entire CASLIBs. That was a silly idea.
*		  15JUL2020 Stu  | v0.7 - Added library checks for the CAS engine: if the user specifies a non-CAS library,
*									 it will warn them that it is not assigned with the CAS engine
*								   - Added notes on how many varchar variables were converted
*								   - Updated warning & error text
*								   - Fixed a bug where the program would continue to try and delete in PROC CASUTIL even if
*									 a library or dataset did not exist
*		  16JUL2020 Stu  | v1.0 - Initial release
*						 	    - Add checks for version requirements
\******************************************************************************/

%macro casload
	(data= 			  /*Datasets to load or append in CAS. Multiple datasets are allowed. Separate datasets with spaces. Input dataset options are not supported.*/
   , casout= 		  /*One or two-level output CAS tables names that correspond with datasets specified in the DATA option.*/
   , casdata= 		  /*Alias for CASOUT*/
   , lib= 			  /*Optional. Read all datasets in LIB. If one-level dataset names are specified with the DATA= option, all one-level dataset names will default to LIB. Default: WORK*/
   , caslib= 	 	  /*Optional. If one-level dataset names are specified with the CASOUT= option, all operations will apply to those datasets in CASLIB. Default: CASUSER*/ 
   , promote=NO		  /*Optional. Promote all datasets unless overridden with output dataset options. Default: NO*/
   , save=YES		  /*Optional. If PROMOTE=YES: Save all datasets to each dataset's CASLIB in .sashdat format. Default: YES*/
   , delete=NO		  /*Optional. Delete all datasets in CASOUT. WARNING: If deletesource=YES (default), the source data will be deleted.*/
   , deletesource=YES /*Optional. If DELETE=YES, delete the associated .sashdat file for casout table.*/
   , append=NO		  /*Optional. Append a SAS dataset to an existing CAS table. Valid values: YES, NO, FORCE. Default: NO*/
   , label=NO		  /*Optional. Keep all variable labels. Default: NO*/
   , lowcase=NO		  /*Optional. Standardize all variables as lower-case. Default: NO*/
   , fastpromote=YES  /*Optional. Load data to a temporary CASLIB, remove the old table, then promote the new table. This will significantly reduce VA report downtime, but will temporarily double the amount of memory usage. Default: YES*/
   , varchar=YES	  /*Optional. Convert all character variables > [minvarcharlen] bytes in length to Varchar. Default: YES*/
   , minvarcharlen=16 /*Optional. Minimum bytes to convert characters to varchars. Default: 16*/
    )
	/ minoperator;
	;
 
	%local i j error;

	%let noteoptions		= %sysfunc(getoption(notes));
	%let syntaxcheckoptions = %sysfunc(getoption(syntaxcheck));

	/***** Basic error checks *****/
	/* Stop immediately if basic conditions are not met */

	/* Check if the user is running a CAS session and abort if not */
	options nonotes;

	/* Check minimum version requirements */
	%if(   (%substr(&sysvlong., 1, 1) NE V AND %substr(&sysvlong., %eval(%index(&sysvlong., M)+1), 1) < 5)
		OR &sysver. < 9.4
	   )
	%then %do;
		%put ERROR: This program is only compatible with SAS Viya and 9.4M5 or higher. Currently installed version: &sysvlong..;
		options &noteoptions.;
		%abort;
	%end;

	options &noteoptions.;

	/* casout and casdata are aliases for convenience. Only one should be used. */
	%if(%bquote(&casout.) NE AND %bquote(&casdata.) NE) %then %do;
		%put ERROR: CASOUT and CASDATA are aliases. Only specify one.;
		%abort;
	%end;

	/* casout and casdata are alisases. Set &casout to be what is specified in &casdata if the user uses casdata. */
	%if(%bquote(&casout.) = AND %bquote(&casdata.) NE) %then %let casout = %superq(casdata);

	/***** End Basic error checks *****/

	/***** Set up macro variables *****/

	/* Strip out extra spaces and carriage returns */
	%let data		= %qsysfunc(compbl(%qsysfunc(compress(%superq(data),%str( ),KNP))));		
	%let casout		= %qsysfunc(compbl(%qsysfunc(compress(%superq(casout),%str( ),KNP))));		

	/* Create temp variable names and remove case sensitivity/blanks from options */
	%let caslib		   = %upcase(%cmpres(&caslib.));	
	%let lib		   = %upcase(%cmpres(&lib.));		  
	%let promote	   = %upcase(%cmpres(&promote.));	  
	%let save		   = %upcase(%cmpres(&save.));		  
	%let delete		   = %upcase(%cmpres(&delete.));		  
	%let deletesource  = %upcase(%cmpres(&deletesource.)); 
	%let append		   = %upcase(%cmpres(&append.));	
	%let label	 	   = %upcase(%cmpres(&label.));	  	
	%let fastpromote   = %upcase(%cmpres(&fastpromote.)); 
	%let lowcase	   = %upcase(%cmpres(&lowcase.));
	%let varchar	   = %upcase(%cmpres(&varchar.));
	%let minvarcharlen = %cmpres(&minvarcharlen.);

	/* Temporary CAS staging library name */
	%let stagecas = STAGECAS;

	/* Initialize variables */
	%let nword_data 	 = %sysfunc(countw(&data., %str( )));		/* Total number of SAS datasets specified by the user */
	%let len_data	 	 = %length(&data.);							/* Total string length of data specified by the user */
	%let len_casout	  	 = %length(&casout.);						/* Total length of casout data specified by the user */
	%let token 	      	 =;	/* Token extracted from a string */
	%let cur_string		 =; /* String built from the current set of tokens */

	%let casout_dlm   	 =; /* Casout with pipe delimiters */
	%let partition_dlm	 =; /* Partition supplied for each casout dataset separated with pipe delimiters */
	%let orderby_dlm	 =; /* Orderby supplied for each casout dataset separated with pipe delimiters */
	%let append_dlm		 =;
	%let promote_dlm	 =;

	%let open_parentheses 	   = 0; /* For parsing casout options. Total number of open parentheses */
	%let close_parentheses 	   = 0; /* For parsing casout options. Total number of close parentheses */
	%let remaining_parentheses = 0; /* For parsing casout options. (Open Parentheses - Close Parentheses) */
	%let flag_options_found	   = 0; /* For parsing casout options. Indicates if options were found. */

	/* Get OS slash */
	%if(&sysscp. = WIN) %then %let _SLSH_ = \;
		%else %let _SLSH_ = /;

	/* Set default lib and output CASLIB */
	%if(&caslib =) %then %let caslib = CASUSER;
	%if(&lib.=)    %then %let lib 	 = WORK;

	/*********************************************/
	/*********************************************/
	/*************** SUB-FUNCTIONS ***************/
	/*********************************************/
	/*********************************************/

	/* Checks the system error flag after a step */
	%macro catchError;
		%let error = &syserr.;
	%mend;

	/* Clean up datasets and reset note options */
	%macro cleanup;
		options nosyntaxcheck nonotes;
		%let syscc=0;
		run;

		proc datasets lib=work nolist nowarn;
			delete ___casload_:
			;
		quit;
		
		/* Clear the STAGECAS library if needed */
		%if(%sysfunc(libref(stagecas)) = 0) %then %do;
			libname stagecas clear;
		%end;

		/* Drop the stagecas CASLIB if needed */
		proc cas;

			table.queryCaslib result=r / caslib="&stagecas.";

			if(r["&stagecas."] = 1) then 
				table.dropCaslib / caslib="&stagecas." quiet=true;
		quit;

		options &noteoptions. &syntaxcheckoptions.;
	%mend cleanup;

	/* Trims leading/trailing spaces, multiple spaces, removes spaces between data options.
	   Example: data(option1 = a option2 = (a b c) )
			->  data(option1=a option2=(a b c))
	*/
		
	/* Modified from: https://blogs.sas.com/content/sasdummy/2013/06/04/find-a-sas-library-engine/ */
	%macro getEngine(libref);
		 %global ENGINE;
	
		 %let dsid=%sysfunc(open(sashelp.vlibnam(where=(libname="%upcase(&libref.)")),i));
	
		 %if(&dsid ^= 0) %then %do;  
		 	%let engnum=%sysfunc(varnum(&dsid,engine));
		   	%let rc=%sysfunc(fetch(&dsid));
		   	%let engine=%cmpres(%sysfunc(getvarc(&dsid,&engnum)));
		   	%let rc= %sysfunc(close(&dsid.));
		 %end;
	
		&engine.
	%mend getEngine;

	%macro cleanDataOptions(string, dlm=%str( ));
		%local i token open_parentheses close_parentheses remaining_parentheses;

		%let string      = %qcmpres(&string);
		%let cleanstring =;
		%let next_token  =;
		%let last_token  =;

		%let open_parentheses 	   = 0;
		%let close_parentheses 	   = 0;
		%let remaining_parentheses = 0;

		/* Run through each token within the string */
		%do i = 1 %to %length(&string.);
			%let token = %qsubstr(&string., &i., 1);

			/* Get the next and last tokens */
			%if(&i. < %length(&string.)) %then %let next_token = %qsubstr(&string., %eval(&i.+1), 1);
			%if(&i. > 1) %then %let last_token = %qsubstr(&string., %eval(&i.-1), 1);
			
			/* Know how many parentheses are left */
			%if(&token. = %str(%() ) %then %let open_parentheses  = %eval(&open_parentheses.+1);
			%if(&token. = %str(%)) ) %then %let close_parentheses = %eval(&close_parentheses.+1);

			%let remaining_parentheses = %eval(&open_parentheses. - &close_parentheses.);

			/* Add a delimiter only in the following conditions:
				1. The current token is a space
				2. The next or previous tokens are  =, (, or )
				3. We are not within an option set
				4. We are not within a sub-option set
				5. The previous token is (, ), or = and the next token is alphanumeric OR;
				   The previous and next tokens are alphanumeric
				   
			   Example: dataset1(option1 = a option2 = (a b c) ) dataset2 dataset3
					->  data(option1=a option2=(a b c))|dataset2|dataset3
			*/
			%if( %eval( (	(&next_token. = %str(=)  OR &last_token. = %str(=)	)
				  		 OR (&next_token. = %str(%() OR &last_token. = %str(%() )
				  		 OR (&next_token. = %str(%)) OR &last_token. = %str(%)) )
				 		 )
				 	   AND &token. = %str( )
			   		  )=0

			   AND %eval( (   (&last_token. = %str(%)) AND %sysfunc(findc(&next_token.,,N)) )
						  	OR (%sysfunc(findc(&last_token.,,N)) AND %sysfunc(findc(&next_token.,,N)) )
						   )
						  AND &token. = %str( )
						  AND &remaining_parentheses. LE 1
						 ) = 0
			   )
			%then %let cleanstring = %str(&cleanstring.&token.);

				/* This handles when we are currently within a dataset's options and 
				   need to add spaces between sub-options or multiple option sets:
				   dataset(option1=a option2=(a b c)):
				*/
				%else %if( (   (&last_token. = %str(%)) AND %sysfunc(findc(&next_token.,,N)) )
						  	OR (%sysfunc(findc(&last_token.,,N)) AND %sysfunc(findc(&next_token.,,N)) )
						   )
						  AND &token. = %str( )
						  AND &remaining_parentheses. = 1
						 )			  	 
				%then %let cleanstring = %str(&cleanstring. );

					/* This handles when we are between datasets
					   dataset1(option1=a option2=(a b c))|dataset2|dataset3

					3. We are not within an option set
					4. We are not within a sub-option set
					5. The previous token is (, ), or = and the next token is alphanumeric OR;
					   The previous and next tokens are alphanumeric
					*/

					%else %if( (   (&last_token. = %str(%)) AND %sysfunc(findc(&next_token.,,N)) )
						  	OR (%sysfunc(findc(&last_token.,,N)) AND %sysfunc(findc(&next_token.,,N)) )
						   )
						  AND &token. = %str( )
						  AND &remaining_parentheses. = 0
						 )			  	 
					%then %let cleanstring = %str(&cleanstring.&dlm.);
		%end;

		%superq(cleanstring)
	%mend cleanDataOptions;

	/* Check if a user has input valid arguments and generate a user-friendly statement if
	   they have not. Default is to output an error and abort.
	*/
	%macro checkValidArgs(option=, args=, type=ERROR, action=ABORT, case=NO) / minoperator;
		%local i n_args;

		/* Upcase options */
		%let case   = %upcase(&case.);
		%let type   = %upcase(&type.);
		%let action = %upcase(&action.);
		%let option = %upcase(&option.);

		/* Enable or disable case-sensitivity */
		%if(&case. = NO) %then %do;
			%let args		= %upcase(&args.);
			%let option_arg = %upcase(&&&option.);
		%end;
			%else %let option_arg = &&&option.;

		/* Only allow notes, warnings, or errors */
		%if(%eval(&type. IN NOTE WARNING ERROR) = 0) %then %do;
			%put ERROR: type= in checkValidArgs can only be ERROR, WARNING, or NOTE.;
			%put WARNING: Irony detected;
			%abort;
		%end;

		/* If the option is invalid, tell the user what options are valid */
		%if(%eval(&option_arg. IN &args.) = 0) %then %do;
			%let n_args = %sysfunc(countw((&args., %str( ))));
			%let arg_friendly =;

			/* Generate the statement:
					args1
					args1 or args2
					args1, args2, or args3
			*/
			%do i = 1 %to &n_args.;%
				%let arg = %scan(&args., &i., %str( ));

				%if(&i. < %eval(&n_args.-1)) %then %let arg_friendly = &arg.,;
					%else %if(&i. = %eval(&n_args.-1) AND &n_args. < 3) %then %let arg_friendly = &arg_friendly. &arg. or;
						%else %if(&i. = %eval(&n_args.-1)) %then %let arg_friendly = &arg_friendly. &arg., or;	/* Oxford comma. I like it, okay, get off my case. */
							%else %if(&i. = &n_args.) %then %let arg_friendly = &arg_friendly. &arg.;
			%end;

			%put &type.: Invalid option &option.=&option_arg. &option. must be &arg_friendly..;
			
			/* If the user specifies to abort, then stop the macro */
			%if(%upcase(&action.) = ABORT) %then %abort;
		%end;
	%mend checkValidArgs;

	/* Parse an option list and save the arguments to a delimited list,
	   &option._dlm

	   Example: dataset1(partition=(a b c)) dataset2(option=(d e f)) dataset3(option=YES)
			-> partition_dlm = a b c|d e f

	   This program only works if %cleanDataOptions has been run first. Use clean=YES to force it to run.
	*/
	%macro getOptionArgs(list, dlm=, option=, default=, clean=NO);
		%local i j
			   token next_token
			   flag_close_parentheses
			   flag_within_parentheses
			   string_buffer
		;

		%let dlm			 = %superq(dlm);
		%let list 			 = %superq(list);
		%let option			 = %upcase(&option.);
		%let clean			 = %upcase(&clean.);
		%let len_option		 = %length(&option.);
		%let outlist		 =;

		/* If no default is specified, set the value to _NO(option name)_ */
		%if(&default. =) %then %let default = _NO&OPTION._;

		/* Clean options list if user specifies */
		%if(&clean. = YES) %then %let list = %cleanDataOptions(&list., dlm=|);

		%let nword_string = %sysfunc(countw(&list., &dlm.));
			
		%do i = 1 %to &nword_string.;
			%let string		     = %qupcase(%qscan(&list., &i., &dlm.));
			%let len_string	     = %length(&string.);
			%let idx_option		 = %index(&string., &option.=);
			%let string_buffer 	 =;
			%let flag_suboptions = 0;
			%let flag_end		 = 0;

			%if(&idx_option.) %then %do;
				%let idx_option_args = %eval(%index(&string., &option.=) + &len_option. + 1;
				%let j = &idx_option_args.;

				/* Remove spaces outside of parentheses */
				%do %until(&flag_end. OR &j. > &len_string.);

					%let token = %qsubstr(&string., &j., 1);

					/* Get the next token */
					%if(&j. < &len_string. ) %then %let next_token = %qsubstr(&string., %eval(&j.+1), 1);
						
					/* Two cases: 1. There are sub-options
								  2. There are not sub-options
					*/
					%if(&token. = %str(%() AND &flag_suboptions. = 0) %then %let flag_suboptions = 1;
 
					%if(&flag_suboptions.) %then %do;
						%if(&token. NE %str(%() AND &token. NE %str(%))) %then %let outlist = &outlist.&token.;
							%else %if(&token. = %str(%)) AND &i. < &nword_string.)
								%then %let outlist = &outlist.&dlm.;
					%end;
						%else %do;

							/* There are no sub-options */
							%if(&token. NE %str(%)) AND &token. NE %str( ) ) %then %let outlist = &outlist.&token.;	
								%else %if(&i. < &nword_string.)	%then %let outlist = &outlist.&dlm.;	
						%end;

					/* The loop is done when:
					   - There are no sub-options and we have encountered a space OR;
					   - There are sub-options and we have encountered a )
					*/
					%let flag_end = %sysevalf(   (&flag_suboptions. = 0 AND &token. = %str( ))
									  	  	  OR (&flag_suboptions. = 1 AND &token. = %str(%)))
											, boolean
					   				     	 );

					%let j = %eval(&j.+1);

					%if(&j. > 5000) %then %do;
						%put ERROR: Infinite loop detected in getOptionArgs. This should not happen. Contact developer.;
						%abort;
					%end;

/*							%put len_string: &len_string.;*/
/*							%put J: &j.;*/
/*							%put STRING: &string.;*/
/*							%put TOKEN: &token.;*/
/*							%put OUTLIST: &outlist.;*/
				%end;
			%end;
				%else %if(&i. < &nword_string.) %then %let outlist = &outlist.&default.&dlm.;
					%else %let outlist = &outlist.&default.;
		%end;

		%superq(outlist)
	%mend getOptionArgs;

	/* Set default option arguments if the user did not specify them*/
	%macro setDefaultDataOptionArgs(list=, dlm=, option=, default=);
		%local string outlist;

		%let list 			 = %superq(list);
		%let option			 = %upcase(&option.);
		%let nword_list		 = %sysfunc(countw(&list., &dlm.));
		%let outlist		 =;

		%if(&default. =) %then %let default = _NO&OPTION_.;

		%do i = 1 %to &nword_list.;
			%let string = %qscan(&list., &i., &dlm.);
			%let n_open_parentheses = %sysfunc(countc(&string., %str(%()));
	
			/* Add default options to if there are no other options*/
			%if(&n_open_parentheses. = 0) %then %let new_string = &string.(&option.=&default.);
				
				/* If there are other options except the option we are interested in, add it to the end of the option list.
				   Otherwise, add the option to the list
				*/
				%else %if(%index(%qupcase(%qsysfunc(compress(&string.))), %str(&option.=)) = 0)
					%then %let new_string = %substr(&string., 1, %eval(%length(&string.)-1)) &option.=&default.);
						%else %let new_string = &string.;

			/* Create the new delimited outlist */
			%if(&i. < &nword_list.) %then %let outlist = &outlist.&new_string.&dlm.;
				%else %let outlist = &outlist.&new_string.;
		%end;

		%superq(outlist)

	%mend setDefaultDataOptionArgs;			
	
	/*********************************************/
	/*********************************************/
	/*************** ERROR CHECKING **************/
	/*********************************************/
	/*********************************************/

	/* Warn if the STAGECAS library is found */
	%if(%sysfunc(libref(STAGECAS) ) = 0) %then %put WARNING: The STAGECAS library is a reserved libname for this program. Unexpected results may occur.;

	/* Error if an integer is not specified for minvarcharlen */
	%if(%datatyp(&minvarcharlen.) = CHAR) %then %do;
		%put ERROR: MINVARCHARLEN must be an integer greater than 0.;
		%abort;
	%end;

	/* Error if an integer >= 0 is not specified for minvarcharlen */
	%if(&minvarcharlen. LE 0 OR %index(&minvarcharlen., .) ) %then %do;
		%put ERROR: MINVARCHARLEN must be an integer greater than 0.;
		%abort;
	%end;

	%if(&minvarcharlen. < 16) %then
		%put WARNING: Setting MINVARCHARLEN < 16 will increase table size.;

	/* Error if name literals are found */
	%if(%index(&data., %str(%') ) ) %then %do;
		%put ERROR: Data name literals are not supported. Please use a valid SAS v7 dataset name.;
		%abort;
	%end;

	%if(%index(&casout., %str(%') ) ) %then %do;
		%put ERROR: CASDATA/CASOUT name literals are not supported. Please use a valid SAS v7 dataset name.;
		%abort;
	%end;

	/* CASLIB must be an 8-character SAS libname */
	%if(&caslib. NE AND %sysfunc(libref(&caslib.)) NE 0) %then %do;
		%put ERROR: The CASLIB &caslib. is not assigned or is not a valid 8-character SAS libname.;
		%abort;
	%end;
		
	/* User must put in a data= dataset if not deleting */
	%if(&data. = AND &lib.= AND &delete. = NO) %then %do;
		%put ERROR: Expected more arguments. Must specify at least one dataset.;
		%abort;
	%end;

	/* User must specify a casout dataset if deleting */
	%if(&casout. = AND &delete. = YES) %then %do;
		%put ERROR: Must specify at least one CASDATA dataset when DELETE=YES.;
		%abort;
	%end;

	/* User must put in a data= dataset if not deleting */
	%if(&data. NE AND &delete. = YES) %then
		%put WARNING: DATA= has no effect when DELETE=YES.
	;

	/* Deleting a dataset will cause append and promote to do nothing */
	%if(&delete. = YES AND (&append. = YES OR promote = YES)) %then %do;
		%put WARNING: APPEND and PROMOTE have no effect when DELETE=YES;
		%let append = NO;
		%let promote = NO;
	%end;

	/* Promoting and appending do not do anything */
	%if(%eval(&append. IN YES FORCE) AND &promote. = YES) %then %do;
		%put WARNING: PROMOTE has no effect when APPEND=&append.;
		%let promote = NO;
	%end;

	/* Check for valid arguments */
	%checkValidArgs(option=save, 		 args=YES NO);
	%checkValidArgs(option=delete,  	 args=YES NO);
	%checkValidArgs(option=append,  	 args=YES NO FORCE);
	%checkValidArgs(option=label, 	 	 args=YES NO);
	%checkValidArgs(option=lowcase, 	 args=YES NO);
	%checkValidArgs(option=deletesource, args=YES NO);

	/* If user specifies an entire library rather than specific datasets, count the number of datasets */
	%if(&data. = AND &delete. = NO) %then %do;

		%if(&lib. NE AND &casout. NE) %then
			%put WARNING: Datasets will be read from &lib. in alphabetical order. CASDATA dataset names may not be valid if new datasets are added to &lib..;

		/* Read all datasets from LIB */
		proc sql noprint;
			select count(*)
				 , cats(libname, '.', memname)
			into :nword_data
			   , :data
			from dictionary.members
			where 	   library = "&lib."
				   AND NOT missing(memname)	/* This can happen sometimes */
			order by memname
			;
		quit;

		%let len_data = %length(&data.);

		/* Abort if no datasets are found */
		%if(&nword_data. = 0) %then %do;
			%put ERROR: No datasets were found in &lib..;
			%abort;
		%end;
	%end;

	/* Check for dataset options (not yet supported) */
	%do i = 1 %to &len_data.;
		%let token = %qsubstr(&data., &i., 1);

		%if(&token. = %str(%() OR &token. = %str(%)) ) %then %do;
			%put ERROR: Input dataset options are not supported.;
			%abort;
		%end;
	%end;

	/* Check for casout options with append (not yet supported) */
	%if(&append. = YES) %then %do;

		%do i = 1 %to &len_casout.;
			%let token = %qsubstr(&casout., &i., 1);

			%if(&token. = %str(%() OR &token. = %str(%)) ) %then %do;
				%put ERROR: CASOUT/CASDATA dataset options are not yet supported when APPEND=YES.;
				%abort;
			%end;
		%end;

	%end;

	/* Clean up casout- cleanDataOptions automatically quotes the result */
	%let casout = %cleanDataOptions(&casout., dlm=|);

	/*********************************************/
	/*********************************************/
	/*********** CASOUT ARGUMENT PARSER **********/
	/*********************************************/
	/*********************************************/

	/* Convert all casout statements into to pipe-delimited values.
	   Tokenize each character and determine if the token is part of an option or not.
	   Options are always in parentheses. If an open parentheses is encountered, then
	   do not add a pipe delimiter to the next space until all open parentheses have a
	   matching close parentheses. e.g.

	   data1(partition=(a b c) orderby=(d) promote=NO append=NO)|data2(promote=NO append=NO)
	*/

	%if(&len_casout. > 0) %then %do;
		%let casout_dlm = %cleanDataOptions(&casout., dlm=|);
	%end;

	/* Count of words in casout_dlm at this point in the program */
	%let nword_casout_dlm = %sysfunc(countw(&casout_dlm., |));

	/* Abort if unmatched parentheses were found */
	%if(&remaining_parentheses. > 0) %then %do;
		%put ERROR: Unmatched parentheses found in the casout statement.;
		%abort;
	%end;

	/* Finally, go through all casout datasets that were not specified and add them to the delimited list */
	%if(&nword_casout_dlm. < &nword_data. AND &delete.=NO) %then %do;
		%do i = %eval(&nword_casout_dlm.+1) %to &nword_data.;
			%let dsn = %scan(&data., &i., %str( ));
			
			/* Extract dataset names and add a default library */
			%if(%scan(&dsn., 2, .) =) %then %let dsn = &caslib..&dsn.;
				%else %let dsn = &caslib..%scan(&dsn., 2, .);

			/* Do not add pipe-delimiter if casout is not specified */
			%if(&i. = 1) %then %let casout_dlm = &dsn.;
				%else %let casout_dlm = &casout_dlm.|&dsn.;
		%end;

		%let casout_dlm	= %superq(casout_dlm);
	%end;

	%let partition_dlm = %getOptionArgs(list=&casout_dlm., dlm=|, option=partition);
	%let orderby_dlm   = %getOptionArgs(list=&casout_dlm., dlm=|, option=orderby);
	%let promote_dlm   = %getOptionArgs(list=&casout_dlm., dlm=|, option=promote, default=&promote.);
	%let append_dlm	   = %getOptionArgs(list=&casout_dlm., dlm=|, option=append, default=&append.);

	/* Set default option arguments for casout options. Check if any are specified. If not, add them. For example:
	   - A user specified promote=YES
	   - A user also specifies promote=NO dataset options for casout data
	   - Promote the datasets without explicit promote statements, but do not promote datasets with explicit promote=NO 
	*/

	%let casout_dlm = %setDefaultDataOptionArgs(list=&casout_dlm., dlm=|, option=promote, default=&promote.);
	%let casout_dlm = %setDefaultDataOptionArgs(list=&casout_dlm., dlm=|, option=append, default=&append.);

/* 	%put **** &casout_dlm.; */
/* 	%put **** &orderby_dlm; */
/* 	%put **** &promote_dlm; */
/* 	%put **** &append_dlm; */

	/* Total number of words in casout_dlm at this point in the program */
	%let nword_casout_dlm = %sysfunc(countw(&casout_dlm., |));

	/* Give a warning if there are more casout datasets than input datasets */
	%if(&nword_casout_dlm. > &nword_data. AND &delete. = NO) 
		%then %put WARNING: User specified more casout datasets than input datasets. These datasets will be ignored.;
	
	/*********************************************/
	/*********************************************/
	/************ FINAL OUTPUT STEPS *************/
	/*********************************************/
	/*********************************************/

	/* If the user is not deleting any datasets:
	   Create a small database of valid datasets that contains:
		- The full two-level dataset name with options
		- Library of the dataset
		- Dataset name
		- The full two-level casout name with options
		- Caslib of the casout dataset
		- Casout dataset name
		- Partition variables
		- Orderby variables

		In the code block below, "&caslib" takes on the value that
		is stored in &caslib_dlm for a given dataset. It is no longer the same value as
		the user-specified value.
	*/

	/* Turn off unneeded notes */
	options nonotes;

	%if(&delete. = NO) %then %do;

		data ___casload_tmp___;
				
			length data    		$40. 
				   lib	 		$8.
				   dsn 			$32.
				   casout  		$1000.
				   caslibname	$8.
				   casdsn		$32.
				   partition
				   orderby		$1000.
				   promote
				   append		$5.
			;

			/* Prevents init notes */
			call missing(of _ALL_);

			nword_data 	= 0;
			n_promote 	= 0;
			n_append 	= 0;

			do i = 1 to &nword_data.;
				data = scan("&data.", i, ' ');
		
				/* Extract library and dataset names */
				if(missing(scan(data, 2, '.'))) then do;
					lib = upcase("&lib.");
					dsn = upcase(scan(data, 1, '('));
				end;
					else do;
						lib = upcase(scan(data, 1, '.'));
						dsn	= upcase(scan(data, 2, '.)' ));
					end;

				/* If the dataset exists then do the same for casout/partition/orderby */
				if(exist(data)) then do;	
					_casout  = scan("&casout_dlm.", i, '|');

					if(missing(scan(_casout, 2, '.'))) then do;
						caslibname = upcase("&caslib.");
						casdsn	   = upcase(scan(_casout, 1, '('));
						casout	   = cats(casdsn, substr(_casout, find(_casout, '(')));
					end; 
						else do;
							caslibname = upcase(scan(_casout, 1, '.'));
							casdsn 	   = upcase(scan(_casout, 2, '.('));
							casout 	   = cats(casdsn, substr(_casout, find(_casout, '(')));
						end;

					partition = scan("&partition_dlm", i, '|');
					orderby	  = scan("&orderby_dlm", i, '|');
					promote	  = scan("&promote_dlm.", i, '|');
					append	  = scan("&append_dlm.", i, '|');

					/* Promote has no effect when append=yes */
					if(promote = 'YES' AND append NE 'NO') then do;
						promote = 'NO';
						put 'WARNING: Promote=' promote 'has no effect on ' casdsn 'when append=YES.';
					end;

					if(promote = 'YES') then n_promote+1;
					if(append NE 'NO') then n_append+1;

					/* Remove library from casout statement */
					if(missing(scan(_casout, 2, '.'))) then casout = _casout;
						else casout = scan(_casout, 2, '.');

					/* Remove the promote dataset option if fastpromote is yes to prevent errors during 
					   the intermediate stage of loading to STAGECAS
					*/
					if(promote = 'YES' AND "&fastpromote." = 'YES') then casout = tranwrd(upcase(casout), 'PROMOTE=YES', 'PROMOTE=NO');

					output;

					nword_data+1;
					load_order+1;
				end;
					else do;
						lib_dsn = cats(lib, '.', dsn);
						lib_dsn_dot = cats(lib, '.', dsn, '.');

						put "ERROR: " lib_dsn "does not exist.";
						put "ERROR: Could not load " lib_dsn_dot;
					end;
			end;

			/* Update the total nmber of valid datasets and count the number of 
			   promotions/appends
			*/
			call symputx('nword_data', nword_data);
			call symputx('n_promote', n_promote);
			call symputx('n_append', n_append);
			call symputx('n_new', n_promote - n_append);

 			drop i nword_data lib_dsn _: n_promote n_append; 
		run;

		/* Check if all datasets are still valid. If not, clean up and quit. */
		%if(&nword_data. = 0) %then %do;
			%put ERROR: No valid datasets exist;
			%cleanup;
			%abort;
		%end;

		/* Get the actual caslib name in case the caslib and sas libname statements do not match */
		proc sql noprint;
			create table ___casload_args___ as
				select t1.*
					 , t2.caslib
				from ___casload_tmp___ as t1
				LEFT JOIN
				(select upcase(libname) as libname
					  , sysvalue 		as caslib
				 from dictionary.libnames
				 where 	engine 			= 'CAS' 
					AND upcase(sysname) = 'CASLIB'
				) as t2
				ON t1.caslibname = t2.libname
				order by load_order
			;
		quit;

		/* Extract all variables into pipe-delimited lists for looping */
		proc sql noprint;
			select data
				 , lib
				 , quote(upcase(lib)) /* Fixes performance issue with dictionary tables and CAS */
				 , dsn
				 , casout
				 , caslib
				 , caslibname
				 , casdsn
				 , partition
				 , orderby
				 , promote
				 , append
			into :data_dlm 	  	  separated by '|'
			   , :lib_dlm 		  separated by '|'
			   , :lib_dlmc		  separated by ',' /* Fixes performance issue with dictionary tables and CAS */
			   , :dsn_dlm		  separated by '|'
			   , :casout_dlm  	  separated by '|'
			   , :caslib_dlm	  separated by '|' /* Actual caslib name */
			   , :caslibname_dlm  separated by '|' /* SAS libname of caslib */
			   , :casdsn_dlm 	  separated by '|'
			   , :partition_dlm   separated by '|'
			   , :orderby_dlm 	  separated by '|'
			   , :promote_dlm	  separated by '|'
			   , :append_dlm	  separated by '|'
			from ___casload_args___
			;

		/* Quote all delimited lists */
		%let data_dlm 	     = %superq(data_dlm);
		%let lib_dlm   	     = %superq(lib_dlm);
		%let dsn_dlm	     = %superq(dsn_dlm);
		%let casout_dlm	     = %superq(casout_dlm);
		%let caslib_dlm	     = %superq(caslib_dlm);		/* Actual caslib name */
		%let caslibname_dlm  = %superq(caslibname_dlm);	/* SAS libname of caslib */
		%let casdsn_dlm	     = %superq(casdsn_dlm);
		%let partition_dlm   = %superq(partition_dlm);
		%let orderby_dlm     = %superq(orderby_dlm);
		%let promote_dlm     = %superq(promote_dlm);
		%let append_dlm	     = %superq(append_dlm);

		/* Get variable length and order. Convert any chars > 16 in length to varchars */
		proc sql noprint;
			create table ___casload_attribs___ as
				select memname
					 , libname
					 , name
					 , type
					 , length
					 , varnum
					 , format
					 , CASE("&label.")
					       when('NO') then ' '
						   else label
					   END as label
				from dictionary.columns 	  as t1
				INNER JOIN
					 ___casload_args___ as t2
				ON 	   t1.libname = t2.lib
				   AND t1.memname = t2.dsn
				where t1.libname IN(&lib_dlmc.)
			;
		quit;

		/* Create a temporary in-memory only CASLIB that is used solely for transferring data.
		   The path does not really exist, but will allow us to create a staging location
		*/
		%if((&n_promote. > 0 AND &fastpromote. = YES) OR &n_append. > 0) %then %do;
			caslib &stagecas. path="&_SLSH_.%sysfunc(uuidgen())" datasource=(srctype="path") notactive;
			libname STAGECAS cas caslib="&stagecas.";
		%end;

		options notes;

		%if(&n_append. = 1)  %then %put NOTE: Appending &n_append. dataset to tables loaded in CAS.;
			%if(&n_append. > 1)  %then %put NOTE: Appending &n_append. datasets to tables loaded in CAS.;

		%if(&n_new. = 1) 	 %then %put NOTE: Loading &n_new. dataset into CAS.;
			%if(&n_new. > 1) 	 %then %put NOTE: Loading &n_new. datasets into CAS.;

		%if(&n_promote. = 1) %then %put NOTE: Promoting &n_promote. dataset.;
			%else %if(&n_promote. > 1) %then %put NOTE: Promoting &n_promote. datasets.;

		%if(&fastpromote. = YES AND &n_promote. > 0) %then %put NOTE: The fast promote method will be used.;

		%put;
	
		%if(&save. = YES AND &n_promote. = 1) %then %put NOTE: The promoted dataset will be saved to disk in .sashdat format.;
			%if(&save. = YES AND &n_promote. > 1) %then %put NOTE: All promoted datasets will be saved to disk in .sashdat format.;

		/* Final output for each dataset */
		%do i = 1 %to &nword_data.;

			%let error = 0;

			options nonotes;

			/* Grab each individual dataset and option value */
			%let dataset    	= %qscan(&data_dlm.,	   	 &i., |);
			%let lib	    	= %qscan(&lib_dlm., 		 &i., |);
			%let dsn	    	= %qscan(&dsn_dlm., 	 	 &i., |);
			%let casout     	= %qscan(&casout_dlm.,  	 &i., |);
			%let outcaslib  	= %qscan(&caslib_dlm.,  	 &i., |);
			%let outcaslibname  = %qscan(&caslibname_dlm., 	 &i., |);
			%let casdsn	    	= %qscan(&casdsn_dlm.,  	 &i., |);
			%let ds_append  	= %qscan(&append_dlm.,	 	 &i., |);
			%let ds_promote 	= %qscan(&promote_dlm.,	 	 &i., |);
			%let partition  	= %qscan(&partition_dlm., 	 &i., |);
			%let orderby    	= %qscan(&orderby_dlm, 		 &i., |);

/* 			%put PROCESSING: &dataset.; */
/* 			%put LIB: &lib.; */
/* 			%put DSN: &dsn; */
/* 			%put CASOUT: &casout; */
/* 			%put OUTCASLIB: &outcaslib; */
/* 			%put CASDSN: &casdsn.; */

			%if(%getEngine(&outcaslibname.) = CAS) %then %do;

				/* Generate length and attrib statements for each variable:
				   length <name> <length>;
				   attrib <name> format=<format>. label="<label>";
	
				   Automatically applies the varchar(len) length type
				   for character variables > 16 bytes
	
				   The length statement automatically sets the variable name and order, and
				   dictates if a variable is lowcase
				*/
				proc sql noprint;
	
				  /* Varchars cannot be declared by attrib in Viya 3.5. Use a separate statement.  */
				  select compbl(cat('length '
								   , CASE("&lowcase.")
						       		 	when('YES') then lowcase(name)
							   		  	else name
						   			 END	
							 	   , CASE
									     when(type='char' AND length GE &minvarcharlen. AND "&varchar."  = "YES") then cat(' varchar(', length, ')')
										 when(type='char' AND (length < &minvarcharlen. OR "&varchar." = "NO")) then cat(' $', length, '.')
										 else cat(' ', length, '.')
									 END
								   , ';'
						 			)
								)
	
					, compbl(cat('attrib '
								, name
							    , CASE 
								      when(NOT missing(format) ) then cat(' format=',format) 
									  else '' 
								  END
							    , CASE(label)
								      when(' ') then cat(' label=', '" "')
									  else cat(' label=', strip(label))
								  END
								, ';'
								)
						    )
	
					, strip(name)
	
					into :lengths separated by ' '
					   , :attribs separated by ' '
					   , :varnames separated by '|'
					from ___casload_attribs___
					where 	 memname = "&dsn."
						 AND libname = "&lib."
					order by varnum
					;
							
					/* Generates temporary names to rename
					   character variables for converting to
					   varchars. All variables being renamed
					   get the temporary name "__char__<number>".
					   These are dropped automatically at the end.
					*/
					select count(*)
						 , name
						 , cats('__char__', monotonic()) as renamechars
						 , cats(name, '=', calculated renamechars)
					into :nvarchars
					   , :varchars separated by ' ' 
					   , :renamechars separated by ' '
					   , :renamestatement separated by ' '
					from ___casload_attribs___
					where 	 memname = "&dsn."
						 AND libname = "&lib."
						 AND type	 = 'char'
						 AND length > &minvarcharlen.
					;
				quit;
	
			/*	%put DATA: &dataset.;*/
			/*	%put LIB: &lib.;*/
			/*	%put DSN: &dsn.; */
			/*	%put CASOUT: &casout.;*/
			/*	%put CASLIB: &caslib.;*/
			/*	%put CASDSN: &casout.;*/
			/*	%put VARCHARS: &varchars;*/
			/*	%put ATTRIBS: %bquote(&attribs.);*/
			/*	%put RENAMECHARS: %bquote(&renamechars.);*/
	
				/*********************************************/
				/*********************************************/
				/*********** CAS OUTPUT: NEW/APPEND **********/
				/*********************************************/
				/*********************************************/
	
				options notes;
	
				/* Remove old CAS table and source if delete is specified and fast promote is off */
				%if( (&ds_promote. = YES AND &fastpromote. = NO AND &ds_append. = NO) ) %then %do;
	
					%put NOTE: Dropping &outcaslib..&casdsn.;
	
					proc casutil incaslib="&outcaslib.";
						droptable casdata="&casdsn." quiet;
					quit;
	
				%end;
	
				/* Load tables to CAS */
	
				/* Append operations are two-stage:
				   1. Get the new data into CASUSER
				   2. Append the new data from CASUSER to &caslib.
				*/
				%if(&ds_append. NE NO) %then %do;
					%put NOTE: &lib..&dsn. will be appended to &outcaslibname..&casdsn.;
	
					%let outcaslibname = STAGECAS;
					%let casout    	   = %scan(&dataset., 2, .);
	
					%put NOTE: STAGE 1: Loading &lib..&dsn. to STAGECAS.;
				%end;
					%else %put NOTE: Loading &casdsn. to &outcaslibname..;
	
				/* Fastpromote: Load to STAGECAS first, then promote it to the final caslib */
				%if(&fastpromote. = YES AND &ds_promote. = YES) %then %let outcaslibname = STAGECAS;
	
				/* Load to CAS and convert chars to varchars.
	
				   If appending: caslib = STAGECAS
				   STEP 1		 casout = &dsn
	
				   Otherwise: 	 caslib = &caslib
								 casout = &casout
				*/
	
				data &outcaslibname..&casout.;
					&lengths.;
					&attribs.;
		
					set &lib..&dsn.(%if(&nvarchars. > 0) %then %do; 
										rename=(&renamestatement.)
							  		%end;
								   );
						%if(&nvarchars. > 0) %then %do;
		
							%do j = 1 %to &nvarchars.;
								%scan(&varchars., &j.) = %scan(&renamechars., &j.);
							%end;
		
							drop &renamechars.;

						%end;
				run;
				
				%catchError;

				/* Give partition/orderby info */
				%if(&error. LE 6) %then %do;

					%if(&partition. NE _NOPARTITION_) %then 
						%put NOTE: &outcaslibname..&dsn. was partitioned by (&partition.).
					;
			
					%if(&orderby. NE _NOORDERBY_) %then 
						%put NOTE: &outcaslibname..&dsn. was ordered by (&orderby.).
					;
			
					%if(&nvarchars. = 1) %then  
						%put NOTE: %cmpres(&nvarchars.) CHAR was automatically converted to VARCHAR.
					;
						%else %if(&nvarchars. > 1) %then
							%put NOTE: %cmpres(&nvarchars.) CHARS were automatically converted to VARCHARS.
						;
				%end;
					%else %put ERROR: Could not load &lib..&dsn..;

			  	/* STEP 2 - APPEND: Append data in STAGECAS to the final caslib */
				%if(&ds_append. NE NO AND &error. LE 6) %then %do;
		
					%let outcaslibname = %qscan(&caslibname_dlm., &i., |);
					%let casdsn    	   = %qscan(&casdsn_dlm., &i., |);
										
					%put NOTE: STEP 2: Appending STAGECAS.&dsn. to &outcaslibname..&casdsn..;
		
					data &outcaslibname..&casdsn.(append=&ds_append.);
						set STAGECAS.&dsn.;
					run;

					%catcherror;

					%if(&error. > 6) %then %put ERROR: Unable to append &lib..&dsn. to &outcaslibname..&casdsn..;

					/* Remove temporary data from STAGECAS */
					proc casutil incaslib="stagecas";
						droptable casdata="&dsn." quiet;
					run;
		
				%end;
			%end;
				%else %do;
					%if(%sysfunc(libref(&outcaslibname.)) NE 0) %then %put WARNING: The CASLIB &outcaslibname. is not assigned.;
						%else %if(%getEngine(&outcaslibname.) NE CAS) %then %put WARNING: The library &outcaslibname. is not assigned with the CAS engine.;
						
					%put ERROR: Could not load &lib..&dsn..;

				%end;
		%end;

		/* After loading all tables, save them if the user specifies */
		%if(&save. = YES AND &error. LE 6 AND (&n_promote. > 0) ) %then %do;
							
			proc casutil;
		
				/* Fast promote */
		
				/* Fast promote: Copy to STAGECAS first, then promote straight to the CASLIB */
				%if(&fastpromote. = YES) %then %do;
		
					%do i = 1 %to &nword_data.;
						%let ds_promote = %qscan(&promote_dlm.,	&i., |);
			
						%if(&ds_promote. = YES) %then %do;
							%let outcaslib	   = %scan(&caslib_dlm., &i., |);
							%let outcaslibname = %scan(&caslibname_dlm., &i., |);
							%let casdsn 	   = %scan(&casdsn_dlm., &i., |);

							%if(%getEngine(&outcaslibname.) = CAS AND %sysfunc(libref(&outcaslibname.)) = 0) %then %do;						

								droptable casdata = "&casdsn." incaslib  = "&outcaslib." quiet;	
								promote casdata = "&casdsn." incaslib = "&stagecas." outcaslib = "&outcaslib.";
				
							%end;
						%end;
					%end;
				%end;
		
				/**** Save Data ****/
				%do i = 1 %to &nword_data.;
		
					%let ds_promote = %qscan(&promote_dlm.,	&i., |);
		
					%if(&ds_promote. = YES) %then %do;
		
						%let outcaslib	   = %scan(&caslib_dlm., &i., |);
						%let outcaslibname = %scan(&caslibname_dlm., &i., |);
						%let casdsn 	   = %scan(&casdsn_dlm., &i., |);
						%let partition     = %scan(&partition_dlm., &i., |);
						%let orderby       = %scan(&orderby_dlm, &i., |);

		 			 	%if(%getEngine(&outcaslibname.) = CAS AND %sysfunc(libref(&outcaslibname.)) = 0) %then %do;

							%put NOTE: Saving &outcaslibname..&casdsn. to disk in .sashdat format.;
								
							/* Let user know that partitions and row order will be saved */
							%if(&partition. NE _NOPARTITION_ AND &orderby. NE _NOORDERBY_) %then %put
								NOTE: Partitions and row order will be saved for &outcaslibname..&casdsn..
							;
								%else %if(&partition. NE _NOPARTITION_) %then %put
									NOTE: Partitions will be saved for &outcaslibname..&casdsn..
								;
									%else %if(&orderby. NE _NOORDERBY_) %then %put
										NOTE: Row order will be saved for &outcaslibname..&casdsn..
									;	
				
							save casdata   = "%lowcase(&casdsn.)"
								 incaslib  = "&outcaslib." 
								 outcaslib = "&outcaslib." 
								 replace
										
								/* Add partition options */
								%if(&partition. NE _NOPARTITION_) %then %do;
									partitionby=(&partition.)
								%end;
			
								/* Add orderby options */
								%if(&orderby. NE _NOORDERBY_) %then %do;
									orderby=(&orderby.)
								%end;
							;
						%end;
					%end;
				%end;
			quit;
		%end;
			%else %if(&error. > 6) %then %do;
				%put ERROR: An error was encountered while loading tables to CAS.;
				%put ERROR: No tables have been saved to disk.;
			%end;
	%end;

		/*********************************************/
		/*********************************************/
		/************** DELETE FROM CAS **************/
		/*********************************************/
		/*********************************************/
		%else %do;

			/* Get the list of datasets to delete */
			data ___casload_tmp___;
				do i = 1 to &nword_casout_dlm.;
					_casout  = scan("&casout_dlm.", i, '|');
					caslibname = upcase("&caslib.");

					if(missing(scan(_casout, 2, '.'))) then do;
						caslibname = upcase("&caslib.");
						casdsn 	   = upcase(scan(_casout, 1, '('));
					end; 
						else do;
							caslibname = upcase(scan(_casout, 1, '.'));
							casdsn 	   = upcase(scan(_casout, 2, '.('));
						end;

					output;
				end;
			run;

			/* Determine the name of the caslibs */
			proc sql noprint;
				select count(*)
					 , t1.casdsn
					 , t2.caslib
					 , t1.caslibname
				into :nword_casout_dlm
				   , :casdsn_dlm separated by '|'
				   , :caslib_dlm separated by '|'
				   , :caslibname_dlm separated by '|'
				from ___casload_tmp___ as t1
			  	LEFT JOIN
					(select upcase(libname) as libname
						  , sysvalue 		as caslib
					 from dictionary.libnames
					 where 	engine 			= 'CAS' 
						AND upcase(sysname) = 'CASLIB'
						AND libname			= "&caslib."
					) as t2
				ON t1.caslibname = t2.libname
				where NOT missing(t2.caslib)
				;
			quit;

			/* Warn the user if any CASLIBs are unassigned or assigned incorrectly */
			proc sql noprint;
				select count(*)
					 , casdsn
					 , caslibname
				into :nword_casout_dlm_all
				   , :casdsn_dlm_all separated by '|'
				   , :caslibname_dlm_all separated by '|'
				from ___casload_tmp___
				;
			quit;

			%do i = 1 %to &nword_casout_dlm_all.;
				%let casdsn 	= %qscan(&casdsn_dlm_all., &i., |);
				%let caslibname = %qscan(&caslibname_dlm_all., &i., |);

				%if(%getEngine(&caslibname.) NE CAS) %then %do;
					%put WARNING: The library &caslibname. is not assigned with the CAS engine.;
					%put ERROR: Unable to delete &caslibname..&casdsn..;
				%end;
			%end;

		/* v0.6 Disable deleting entire CASLIBs. The hell was I thinking? */
		/* If casout was not specified but caslib is, get the list of tables from the caslib */
		/*				%else %if(&casout. = AND &caslib. NE) %do;*/
		/**/
		/*					proc sql ;*/
		/*						select count(*)*/
		/*							 , t1.memname*/
		/*							 , t2.caslib*/
		/*						into :nword_casout_dlm*/
		/*						   , :casdsn_dlm separated by '|'*/
		/*						   , :caslib_dlm separated by '|'*/
		/*						from (select memname*/
		/*								   , libname*/
		/*							  from dictionary.members*/
		/*							  where 	libname = "&caslib."*/
		/*									AND NOT missing(memname)*/
		/*							 ) as t1*/
		/*					  	LEFT JOIN*/
		/*							(select upcase(libname) as libname*/
		/*								  , sysvalue 		as caslib*/
		/*							 from dictionary.libnames*/
		/*							 where 	engine 			= 'CAS' */
		/*								AND upcase(sysname) = 'CASLIB'*/
		/*								AND libname			= "&caslib."*/
		/*							) as t2*/
		/*						ON  t1.libname = t2.libname*/
		/*						;*/
		/*					quit;*/
		/*			%end;*/
											
			options notes;

				/* v0.6: This is disabled */
	/* 			%if(&casout. = AND &caslib. NE) %then %do; */
	/*  */
	/* 				%put NOTE: All in-memory tables in &caslib. will be dropped.; */
	/* 				%if(&deletesource. = YES) %then %put NOTE: All on-disk tables in &caslibname. will be deleted.; */
	/* 				%put; */
	/*  */
	/* 				%if(&nword_casout_dlm. = 0) %then %put WARNING: &caslibname. has no tables loaded to memory.; */
	/* 			%end; */

			%if(&nword_casout_dlm. > 0) %then %do;
			
				/***** Delete data *****/
				proc casutil;
					%do i = 1 %to &nword_casout_dlm.;

						%let casdsn 	= %qscan(&casdsn_dlm., &i., |);
						%let caslib 	= %qscan(&caslib_dlm., &i, |);
						%let caslibname = %qscan(&caslibname_dlm., &i., |);

						%if(&deletesource. = NO) %then %put NOTE: Dropping &caslibname..&casdsn. from memory.;
							%else %put NOTE: Dropping &caslibname..&casdsn. from memory and deleting %lowcase(&casdsn.).sashdat from disk.;
							
						%if(%sysfunc(exist(&caslibname..&casdsn.))) %then %do;
				
							/* Drop the table from memory */
							droptable casdata="&casdsn."  incaslib="&caslib." quiet;

						%end;
							%else %do;

								%if(&deletesource. = YES) %then %put WARNING: &caslibname..&casdsn. is not loaded to CAS. Data will still be deleted from disk.;
									%else %put WARNING: &caslibname..&casdsn. is not loaded to CAS.;

							%end;
						
						/* Delete the source table from CAS */
						%if(&deletesource. = YES) %then %do;
							deletesource casdata="%lowcase(&casdsn.)" incaslib="&caslib." quiet;
						%end;

					%end;

				quit;

			%end;
				%else %put WARNING: No CAS tables were found in memory. Check your CASDATA tables and try again.;
		%end;
	
	%cleanup;
%mend casload;