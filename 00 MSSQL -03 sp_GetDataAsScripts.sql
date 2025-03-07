ALTER PROCEDURE [staging].[sp_GetDataAsScripts]
	@log_mode bit = 1,			-- If @log_mode is set to 1, the SQL statements constructed by this procedure will be inserted into a temporary table #CollectMetadata
	@result_mode tinyint = 0,	-- If @result_mode is set to 0, then it output all the SELECT statements at end of this procedure
	@table_name varchar(776),  		-- The table/view for which the INSERT statements will be generated using the existing data
	@target_table varchar(776) = NULL, 	-- Use this parameter to specify a different table name into which the data will be inserted
	@include_column_list bit = 1,		-- Use this parameter to include/ommit column list in the generated INSERT statement
	@from varchar(800) = NULL, 		-- Use this parameter to filter the rows based on a filter condition (using WHERE)
	@include_timestamp bit = 1, 		-- Specify 1 for this parameter, if you want to include the TIMESTAMP/ROWVERSION column's data in the INSERT statement
	@debug_mode bit = 0,			-- If @debug_mode is set to 1, the SQL statements constructed by this procedure will be printed for later examination
	@owner varchar(64),		-- Use this parameter if you are not the owner of the table
	@ommit_images bit = 0,			-- Use this parameter to generate INSERT statements by omitting the 'image' columns
	@ommit_identity bit = 0,		-- Use this parameter to ommit the identity columns
	@top int = NULL,			-- Use this parameter to generate INSERT statements only for the TOP n rows
	@cols_to_include varchar(8000) = NULL,	-- List of columns to be included in the INSERT statement
	@cols_to_exclude varchar(8000) = NULL,	-- List of columns to be excluded from the INSERT statement
	@disable_constraints bit = 0,		-- When 1, disables foreign key constraints and enables them after the INSERT statements
	@ommit_computed_cols bit = 0		-- When 1, computed columns will not be included in the INSERT statement
/*
	EXEC [staging].[sp_GetDataAsScripts] @table_name = '', @owner = '', @result_mode = 2, @cols_to_exclude = '''include_from'', ''include_to'',''included_from'',''included_to'',''valid_from'', ''valid_to'''
*/
AS
BEGIN

	SET NOCOUNT ON 
	BEGIN TRY
		IF OBJECT_ID('tempdb..#QueryBuilder') IS NOT NULL DROP TABLE #QueryBuilder;
		IF OBJECT_ID('tempdb..#CollectMetadata') IS NOT NULL DROP TABLE #CollectMetadata;
		CREATE TABLE #CollectMetadata
		(
			ID INT Identity(1,1),
			Section VARCHAR(50),
			[Order] VARCHAR(5),
			String VARCHAR(MAX)
		)

		SELECT 
			'BEGIN TRANSACTION;
		BEGIN TRY
			PRINT CHAR(13) + CHAR(10) + CHAR(13) + CHAR(10) + ''BEGIN:: Processing for [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX]...''
			IF OBJECT_ID(''tempdb..#stg_XXXXXXXXXXXXXXXXXXXXXXXXXXXX'') IS NOT NULL DROP TABLE [#stg_XXXXXXXXXXXXXXXXXXXXXXXXXXXX]
			SELECT 
				*,
				''2024-01-01 00:00:00.000'' AS [valid_from],
				''2501-12-31 00:00:00.000'' AS [valid_to]
			INTO [#stg_XXXXXXXXXXXXXXXXXXXXXXXXXXXX]
			FROM
			(
				BBBBBBBBBBBBBBBBBBBBBBBBBBBB
			) Source
			WHERE Source.[YYYYYYYYYYYYYYYYYYYYYYYYYYYY] IS NOT NULL
			PRINT ''Data inserted into Temporary Staging...''

			DELETE M1
			FROM [#stg_XXXXXXXXXXXXXXXXXXXXXXXXXXXX] M2
			JOIN [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX] M1
				ON M1.YYYYYYYYYYYYYYYYYYYYYYYYYYYY = M2.YYYYYYYYYYYYYYYYYYYYYYYYYYYY
				AND M1.YYYYYYYYYYYYYYYYYYYYYYYYYYYY <> M2.YYYYYYYYYYYYYYYYYYYYYYYYYYYY
			PRINT ''Deleted NOT MATCHED data from Target Table...''

			SET IDENTITY_INSERT [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX] ON
			MERGE INTO [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX] AS Target
			USING [#stg_XXXXXXXXXXXXXXXXXXXXXXXXXXXX] AS Source
			ON 1 = 1
				AND Target.[YYYYYYYYYYYYYYYYYYYYYYYYYYYY] = Source.[YYYYYYYYYYYYYYYYYYYYYYYYYYYY]
				AND Target.[YYYYYYYYYYYYYYYYYYYYYYYYYYYY] = Source.[YYYYYYYYYYYYYYYYYYYYYYYYYYYY]

			WHEN MATCHED THEN
			UPDATE SET
		WWWWWWWWWWWWWWWWWWWWWWWWWWWW

			WHEN NOT MATCHED BY TARGET AND Source.[YYYYYYYYYYYYYYYYYYYYYYYYYYYY] IS NOT NULL THEN
			INSERT AAAAAAAAAAAAAAAAAAAAAAAAAAAA
			VALUES AAAAAAAAAAAAAAAAAAAAAAAAAAAA;

			SET IDENTITY_INSERT [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX] OFF
			--SELECT * FROM [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX]

			DROP TABLE [#stg_XXXXXXXXXXXXXXXXXXXXXXXXXXXX];

			COMMIT TRANSACTION;
			PRINT ''SUCCESS:: Committed data to [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX].''
		END TRY
		BEGIN CATCH
			PRINT ''FAILED:: Rollback [SSSSSSSSSSSSSSSSSSSSSSSSSSSS].[XXXXXXXXXXXXXXXXXXXXXXXXXXXX]; Error: '' + ERROR_MESSAGE()
			ROLLBACK TRANSACTION;
		END CATCH;
		GO' AS TemplateQuery,
		CAST(NULL AS VARCHAR(MAX)) AS UpdatedQuery
		INTO #QueryBuilder
		
		/***********************************************************************************************************
		Example 1:	To generate INSERT statements for table 'titles':
				EXEC sp_generate_inserts 'titles'

		Example 2: 	To ommit the column list in the INSERT statement: (Column list is included by default)
				IMPORTANT: If you have too many columns, you are advised to ommit column list, as shown below,
				to avoid erroneous results
				
				EXEC sp_generate_inserts 'titles', @include_column_list = 0

		Example 3:	To generate INSERT statements for 'titlesCopy' table from 'titles' table:

				EXEC sp_generate_inserts 'titles', 'titlesCopy'

		Example 4:	To generate INSERT statements for 'titles' table for only those titles 
				which contain the word 'Computer' in them:
				NOTE: Do not complicate the FROM or WHERE clause here. It's assumed that you are good with T-SQL if you are using this parameter

				EXEC sp_generate_inserts 'titles', @from = "from titles where title like '%Computer%'"

		Example 5: 	To specify that you want to include TIMESTAMP column's data as well in the INSERT statement:
				(By default TIMESTAMP column's data is not scripted)

				EXEC sp_generate_inserts 'titles', @include_timestamp = 1

		Example 6:	To print the debug information:
		
				EXEC sp_generate_inserts 'titles', @debug_mode = 1

		Example 7: 	If you are not the owner of the table, use @owner parameter to specify the owner name
				To use this option, you must have SELECT permissions on that table

				EXEC sp_generate_inserts Nickstable, @owner = 'Nick'

		Example 8: 	To generate INSERT statements for the rest of the columns excluding images
				When using this otion, DO NOT set @include_column_list parameter to 0.

				EXEC sp_generate_inserts imgtable, @ommit_images = 1

		Example 9: 	To generate INSERT statements excluding (ommiting) IDENTITY columns:
				(By default IDENTITY columns are included in the INSERT statement)

				EXEC sp_generate_inserts mytable, @ommit_identity = 1

		Example 10: 	To generate INSERT statements for the TOP 10 rows in the table:
				
				EXEC sp_generate_inserts mytable, @top = 10

		Example 11: 	To generate INSERT statements with only those columns you want:
				
				EXEC sp_generate_inserts titles, @cols_to_include = "'title','title_id','au_id'"

		Example 12: 	To generate INSERT statements by omitting certain columns:
				
				EXEC sp_generate_inserts titles, @cols_to_exclude = "'title','title_id','au_id'"

		Example 13:	To avoid checking the foreign key constraints while loading data with INSERT statements:
				
				EXEC sp_generate_inserts titles, @disable_constraints = 1

		Example 14: 	To exclude computed columns from the INSERT statement:
				EXEC sp_generate_inserts MyTable, @ommit_computed_cols = 1
		***********************************************************************************************************/

		SET NOCOUNT ON

		--Making sure user only uses either @cols_to_include or @cols_to_exclude
		IF ((@cols_to_include IS NOT NULL) AND (@cols_to_exclude IS NOT NULL))
			BEGIN
				RAISERROR('Use either @cols_to_include or @cols_to_exclude. Do not use both the parameters at once',16,1)
				SELECT -1 --Failure. Reason: Both @cols_to_include and @cols_to_exclude parameters are specified
			END

		--Making sure the @cols_to_include and @cols_to_exclude parameters are receiving values in proper format
		IF ((@cols_to_include IS NOT NULL) AND (PATINDEX('''%''',@cols_to_include) = 0))
			BEGIN
				RAISERROR('Invalid use of @cols_to_include property',16,1)
				PRINT 'Specify column names surrounded by single quotes and separated by commas'
				PRINT 'Eg: EXEC sp_generate_inserts titles, @cols_to_include = "''title_id'',''title''"'
				SELECT -1 --Failure. Reason: Invalid use of @cols_to_include property
			END

		IF ((@cols_to_exclude IS NOT NULL) AND (PATINDEX('''%''',@cols_to_exclude) = 0))
			BEGIN
				RAISERROR('Invalid use of @cols_to_exclude property',16,1)
				PRINT 'Specify column names surrounded by single quotes and separated by commas'
				PRINT 'Eg: EXEC sp_generate_inserts titles, @cols_to_exclude = "''title_id'',''title''"'
				SELECT -1 --Failure. Reason: Invalid use of @cols_to_exclude property
			END


		--Checking to see if the database name is specified along wih the table name
		--Your database context should be local to the table for which you want to generate INSERT statements
		--specifying the database name is not allowed
		IF (PARSENAME(@table_name,3)) IS NOT NULL
			BEGIN
				RAISERROR('Do not specify the database name. Be in the required database and just specify the table name.',16,1)
				SELECT -1 --Failure. Reason: Database name is specified along with the table name, which is not allowed
			END

		--Checking for the existence of 'user table' or 'view'
		--This procedure is not written to work on system tables
		--To script the data in system tables, just create a view on the system tables and script the view instead

		IF @owner IS NULL
			BEGIN
				IF ((OBJECT_ID(@table_name,'U') IS NULL) AND (OBJECT_ID(@table_name,'V') IS NULL)) 
					BEGIN
						RAISERROR('User table or view not found.',16,1)
						PRINT 'You may see this error, if you are not the owner of this table or view. In that case use @owner parameter to specify the owner name.'
						PRINT 'Make sure you have SELECT permission on that table or view.'
						SELECT -1 --Failure. Reason: There is no user table or view with this name
					END
			END
		ELSE
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @table_name AND (TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW') AND TABLE_SCHEMA = @owner)
					BEGIN
						RAISERROR('User table or view not found.',16,1)
						PRINT 'You may see this error, if you are not the owner of this table. In that case use @owner parameter to specify the owner name.'
						PRINT 'Make sure you have SELECT permission on that table or view.'
						SELECT -1 --Failure. Reason: There is no user table or view with this name		
					END
			END

		--Variable declarations
		DECLARE		@Column_ID int, 		
				@Column_List varchar(8000), 
				@Column_Name varchar(128), 
				@Start_Insert varchar(786), 
				@Data_Type varchar(128), 
				@Actual_Values varchar(8000),	--This is the string that will be finally executed to generate INSERT statements
				@IDN varchar(128)		--Will contain the IDENTITY column's name in the table

		--Variable Initialization
		SET @IDN = ''
		SET @Column_ID = 0
		SET @Column_Name = ''
		SET @Column_List = ''
		SET @Actual_Values = ''

		IF @owner IS NULL 
			BEGIN
				SET @Start_Insert = 'INSERT INTO ' + '[' + RTRIM(COALESCE(@target_table,@table_name)) + ']' 
			END
		ELSE
			BEGIN
				SET @Start_Insert = 'INSERT ' + '[' + LTRIM(RTRIM(@owner)) + '].' + '[' + RTRIM(COALESCE(@target_table,@table_name)) + ']' 		
			END


		--To get the first column's ID

		SELECT	@Column_ID = MIN(ORDINAL_POSITION) 	
		FROM	INFORMATION_SCHEMA.COLUMNS (NOLOCK) 
		WHERE 	TABLE_NAME = @table_name AND
		(@owner IS NULL OR TABLE_SCHEMA = @owner)



		--Loop through all the columns of the table, to get the column names and their data types
		WHILE @Column_ID IS NOT NULL
			BEGIN
				SELECT 	@Column_Name = QUOTENAME(COLUMN_NAME), 
				@Data_Type = DATA_TYPE 
				FROM 	INFORMATION_SCHEMA.COLUMNS (NOLOCK) 
				WHERE 	ORDINAL_POSITION = @Column_ID AND 
				TABLE_NAME = @table_name AND
				(@owner IS NULL OR TABLE_SCHEMA = @owner)



				IF @cols_to_include IS NOT NULL --Selecting only user specified columns
				BEGIN
					IF CHARINDEX( '''' + SUBSTRING(@Column_Name,2,LEN(@Column_Name)-2) + '''',@cols_to_include) = 0 
					BEGIN
						GOTO SKIP_LOOP
					END
				END

				IF @cols_to_exclude IS NOT NULL --Selecting only user specified columns
				BEGIN
					IF CHARINDEX( '''' + SUBSTRING(@Column_Name,2,LEN(@Column_Name)-2) + '''',@cols_to_exclude) <> 0 
					BEGIN
						GOTO SKIP_LOOP
					END
				END

				--Making sure to output SET IDENTITY_INSERT ON/OFF in case the table has an IDENTITY column
				IF (SELECT COLUMNPROPERTY( OBJECT_ID(QUOTENAME(COALESCE(@owner,USER_NAME())) + '.' + @table_name),SUBSTRING(@Column_Name,2,LEN(@Column_Name) - 2),'IsIdentity')) = 1 
				BEGIN
					IF @ommit_identity = 0 --Determing whether to include or exclude the IDENTITY column
						SET @IDN = @Column_Name
					ELSE
						GOTO SKIP_LOOP			
				END
				
				--Making sure whether to output computed columns or not
				IF @ommit_computed_cols = 1
				BEGIN
					IF (SELECT COLUMNPROPERTY( OBJECT_ID(QUOTENAME(COALESCE(@owner,USER_NAME())) + '.' + @table_name),SUBSTRING(@Column_Name,2,LEN(@Column_Name) - 2),'IsComputed')) = 1 
					BEGIN
						GOTO SKIP_LOOP					
					END
				END
				
				--Tables with columns of IMAGE data type are not supported for obvious reasons
				IF(@Data_Type in ('image'))
					BEGIN
						IF (@ommit_images = 0)
							BEGIN
								RAISERROR('Tables with image columns are not supported.',16,1)
								PRINT 'Use @ommit_images = 1 parameter to generate INSERTs for the rest of the columns.'
								PRINT 'DO NOT ommit Column List in the INSERT statements. If you ommit column list using @include_column_list=0, the generated INSERTs will fail.'
								SELECT -1 --Failure. Reason: There is a column with image data type
							END
						ELSE
							BEGIN
							GOTO SKIP_LOOP
							END
					END

				--Determining the data type of the column and depending on the data type, the VALUES part of
				--the INSERT statement is generated. Care is taken to handle columns with NULL values. Also
				--making sure, not to lose any data from flot, real, money, smallmomey, datetime columns
				SET @Actual_Values = @Actual_Values  +
				CASE 
					WHEN @Data_Type IN ('char','varchar','nchar','nvarchar') 
						THEN 
							'COALESCE('''''''' + REPLACE(RTRIM(' + @Column_Name + '),'''''''','''''''''''')+'''''''',''NULL'')'
					WHEN @Data_Type IN ('date') 
						THEN 
							'COALESCE('''''''' + RTRIM(CONVERT(char,' + @Column_Name + ',20))+'''''''',''NULL'')'
					WHEN @Data_Type IN ('datetime','datetime2','smalldatetime') 
						THEN 
							'COALESCE('''''''' + RTRIM(CONVERT(char,' + @Column_Name + ',109))+'''''''',''NULL'')'
					WHEN @Data_Type IN ('uniqueidentifier') 
						THEN  
							'COALESCE('''''''' + REPLACE(CONVERT(char(255),RTRIM(' + @Column_Name + ')),'''''''','''''''''''')+'''''''',''NULL'')'
					WHEN @Data_Type IN ('text','ntext') 
						THEN  
							'COALESCE('''''''' + REPLACE(CONVERT(char(8000),' + @Column_Name + '),'''''''','''''''''''')+'''''''',''NULL'')'					
					WHEN @Data_Type IN ('binary','varbinary') 
						THEN  
							'COALESCE(RTRIM(CONVERT(char,' + 'CONVERT(int,' + @Column_Name + '))),''NULL'')'  
					WHEN @Data_Type IN ('timestamp','rowversion') 
						THEN  
							CASE 
								WHEN @include_timestamp = 0 
									THEN 
										'''DEFAULT''' 
									ELSE 
										'COALESCE(RTRIM(CONVERT(char,' + 'CONVERT(int,' + @Column_Name + '))),''NULL'')'  
							END
					WHEN @Data_Type IN ('float','real','money','smallmoney')
						THEN
							'COALESCE(LTRIM(RTRIM(' + 'CONVERT(char, ' +  @Column_Name  + ',2)' + ')),''NULL'')' 
					ELSE 
						'COALESCE(LTRIM(RTRIM(' + 'CONVERT(char, ' +  @Column_Name  + ')' + ')),''NULL'')' 
				END   + '+' +  ''',''' + ' + '
				
				--Generating the column list for the INSERT statement
				SET @Column_List = @Column_List +  @Column_Name + ','	

				SKIP_LOOP: --The label used in GOTO

				SELECT 	@Column_ID = MIN(ORDINAL_POSITION) 
				FROM 	INFORMATION_SCHEMA.COLUMNS (NOLOCK) 
				WHERE 	TABLE_NAME = @table_name AND 
				ORDINAL_POSITION > @Column_ID AND
				(@owner IS NULL OR TABLE_SCHEMA = @owner)


			--Loop ends here!
			END

		--To get rid of the extra characters that got concatenated during the last run through the loop
		SET @Column_List = LEFT(@Column_List,len(@Column_List) - 1)
		SET @Actual_Values = LEFT(@Actual_Values,len(@Actual_Values) - 6)

		IF LTRIM(@Column_List) = '' 
			BEGIN
				RAISERROR('No columns to select. There should at least be one column to generate the output',16,1)
				SELECT -1 --Failure. Reason: Looks like all the columns are ommitted using the @cols_to_exclude parameter
			END

		--Forming the final string that will be executed, to output the INSERT statements
		IF (@include_column_list <> 0)
		BEGIN
			IF (@log_mode = 1)
			BEGIN
				INSERT #CollectMetadata (Section,[Order],String)
				SELECT 'SCHEMA' AS Section, 'L00-A' AS [Order], @owner AS String UNION ALL
				SELECT 'TABLE' AS Section, 'L00-B' AS [Order], @table_name AS String UNION ALL
				SELECT 'HEADERS' AS Section, 'L01-1' AS [Order], @Start_Insert AS String UNION ALL
				SELECT 'HEADERS' AS Section, 'L01-2' AS [Order], RTRIM(@Column_List) AS String UNION ALL
				SELECT 'INSERT' AS Section, 'L02-1' AS [Order], CONCAT(@Start_Insert, ' (' + RTRIM(@Column_List) +  ')' ) UNION ALL
				SELECT 'DATA' AS Section, 'L02-1' AS [Order], CONCAT('SELECT ', REPLACE(@Column_List, '[', ' NULL AS ['), ' UNION ALL ') AS String 

				SET @Actual_Values = 
					'INSERT #CollectMetadata (Section,[Order],String) ' +
					'SELECT ''Data'', ''L02-2'', ' +  
						CASE WHEN @top IS NULL OR @top < 0 THEN '' ELSE ' TOP ' + LTRIM(STR(@top)) + ' ' END + 
						' +''SELECT ''+ ' +  @Actual_Values  + '+'' UNION ALL ''' + ' ' + 
						COALESCE(@from,' FROM ' + CASE WHEN @owner IS NULL THEN '' ELSE '[' + LTRIM(RTRIM(@owner)) + '].' END + '[' + rtrim(@table_name) + ']' + '(NOLOCK)')
			END
			ELSE
			BEGIN
				SELECT CONCAT(@Start_Insert, ' (' + RTRIM(@Column_List) +  ')' + CHAR(13) + CHAR(10) + 'SELECT ', REPLACE(@Column_List, '[', ' NULL AS ['), ' UNION ALL ')
				SELECT CONCAT(@Start_Insert, ' (' + RTRIM(@Column_List) +  ')' + CHAR(13) + CHAR(10) + 'SELECT ', REPLACE(@Column_List, '[', ' NULL AS ['), ' UNION ALL ')
				SELECT @Start_Insert UNION ALL
				SELECT '(' + RTRIM(@Column_List) +  ')' UNION ALL
				SELECT CONCAT('SELECT ', REPLACE(@Column_List, '[', ' NULL AS ['), ' UNION ALL ')

				SET @Actual_Values = 
					'SELECT ' +  
					CASE WHEN @top IS NULL OR @top < 0 THEN '' ELSE ' TOP ' + LTRIM(STR(@top)) + ' ' END + 
					' +''SELECT ''+ ' +  @Actual_Values  + '+'' UNION ALL ''' + ' ' + 
					COALESCE(@from,' FROM ' + CASE WHEN @owner IS NULL THEN '' ELSE '[' + LTRIM(RTRIM(@owner)) + '].' END + '[' + rtrim(@table_name) + ']' + '(NOLOCK)')
			END
		END
		ELSE IF (@include_column_list = 0)
		BEGIN
			IF (@log_mode = 1)
			BEGIN
				INSERT #CollectMetadata (Section,[Order],String)
				SELECT 'SCHEMA' AS Section, 'L00-A' AS [Order], @owner AS String UNION ALL
				SELECT 'TABLE' AS Section, 'L00-B' AS [Order], @table_name AS String UNION ALL
				SELECT 'HEADERS' AS Section, 'L01-1' AS [Order], @Start_Insert AS String UNION ALL
				SELECT 'HEADERS' AS Section, 'L01-2' AS [Order], RTRIM(@Column_List) AS String UNION ALL
				SELECT 'INSERT' AS Section, 'L02-1' AS [Order], CONCAT(@Start_Insert, ' (' + RTRIM(@Column_List) +  ')' ) UNION ALL
				SELECT 'DATA' AS Section, 'L02-1' AS [Order], CONCAT('SELECT ', REPLACE(@Column_List, '[', ' NULL AS ['), ' UNION ALL ') AS String 

				SET @Actual_Values = 
					'INSERT #CollectMetadata (Section,[Order],String) ' +
					'SELECT ''Data'', ''L02-2'', ' +  
						CASE WHEN @top IS NULL OR @top < 0 THEN '' ELSE ' TOP ' + LTRIM(STR(@top)) + ' ' END + 
						' +''SELECT ''+ ' +  @Actual_Values  + '+'' UNION ALL ''' + ' ' + 
						COALESCE(@from,' FROM ' + CASE WHEN @owner IS NULL THEN '' ELSE '[' + LTRIM(RTRIM(@owner)) + '].' END + '[' + rtrim(@table_name) + ']' + '(NOLOCK)')
			END
			ELSE
			BEGIN
				SELECT @Start_Insert UNION ALL
				SELECT CONCAT('SELECT ', REPLACE(@Column_List, '[', ' NULL AS ['), ' UNION ALL ')

				SET @Actual_Values = 
					'SELECT ' + 
					CASE WHEN @top IS NULL OR @top < 0 THEN '' ELSE ' TOP ' + LTRIM(STR(@top)) + ' ' END + 
					' +''SELECT ''+ ' +  @Actual_Values + '+'' UNION ALL ''' + ' ' + 
					COALESCE(@from,' FROM ' + CASE WHEN @owner IS NULL THEN '' ELSE '[' + LTRIM(RTRIM(@owner)) + '].' END + '[' + rtrim(@table_name) + ']' + '(NOLOCK)')
			END
		END

		--Determining whether to ouput any debug information
		IF @debug_mode =1
			BEGIN
				PRINT '/*****START OF DEBUG INFORMATION*****'
				PRINT 'Beginning of the INSERT statement:'
				PRINT @Start_Insert
				PRINT ''
				PRINT 'The column list:'
				PRINT @Column_List
				PRINT ''
				PRINT 'The SELECT statement executed to generate the INSERTs'
				PRINT @Actual_Values
				PRINT ''
				PRINT '*****END OF DEBUG INFORMATION*****/'
				PRINT ''
			END
				
		PRINT '--INSERTs generated by ''sp_generate_inserts'' stored procedure written by Vyas'
		PRINT '--Build number: 22'
		PRINT '--Problems/Suggestions? Contact Vyas @ vyaskn@hotmail.com'
		PRINT '--http://vyaskn.tripod.com'
		PRINT ''
		PRINT 'SET NOCOUNT ON'
		PRINT ''


		--Determining whether to print IDENTITY_INSERT or not
		IF (@IDN <> '')
			BEGIN
				PRINT 'SET IDENTITY_INSERT ' + QUOTENAME(COALESCE(@owner,USER_NAME())) + '.' + QUOTENAME(@table_name) + ' ON'
				PRINT 'GO'
				PRINT ''
			END


		IF @disable_constraints = 1 AND (OBJECT_ID(QUOTENAME(COALESCE(@owner,USER_NAME())) + '.' + @table_name, 'U') IS NOT NULL)
			BEGIN
				IF @owner IS NULL
					BEGIN
						SELECT 	'ALTER TABLE ' + QUOTENAME(COALESCE(@target_table, @table_name)) + ' NOCHECK CONSTRAINT ALL' AS '--Code to disable constraints temporarily'
					END
				ELSE
					BEGIN
						SELECT 	'ALTER TABLE ' + QUOTENAME(@owner) + '.' + QUOTENAME(COALESCE(@target_table, @table_name)) + ' NOCHECK CONSTRAINT ALL' AS '--Code to disable constraints temporarily'
					END

				PRINT 'GO'
			END

		PRINT ''
		PRINT 'PRINT ''Inserting values into ' + '[' + RTRIM(COALESCE(@target_table,@table_name)) + ']' + ''''


		--All the hard work pays off here!!! You'll get your INSERT statements, when the next line executes!
		EXEC (@Actual_Values)

		PRINT 'PRINT ''Done'''
		PRINT ''


		IF @disable_constraints = 1 AND (OBJECT_ID(QUOTENAME(COALESCE(@owner,USER_NAME())) + '.' + @table_name, 'U') IS NOT NULL)
			BEGIN
				IF @owner IS NULL
					BEGIN
						SELECT 	'ALTER TABLE ' + QUOTENAME(COALESCE(@target_table, @table_name)) + ' CHECK CONSTRAINT ALL'  AS '--Code to enable the previously disabled constraints'
					END
				ELSE
					BEGIN
						SELECT 	'ALTER TABLE ' + QUOTENAME(@owner) + '.' + QUOTENAME(COALESCE(@target_table, @table_name)) + ' CHECK CONSTRAINT ALL' AS '--Code to enable the previously disabled constraints'
					END

				PRINT 'GO'
			END

		PRINT ''
		IF (@IDN <> '')
			BEGIN
				PRINT 'SET IDENTITY_INSERT ' + QUOTENAME(COALESCE(@owner,USER_NAME())) + '.' + QUOTENAME(@table_name) + ' OFF'
				PRINT 'GO'
			END

		PRINT 'SET NOCOUNT OFF'


		SET NOCOUNT OFF


		--update Schema Name
		UPDATE A
		SET
			UpdatedQuery = REPLACE(TemplateQuery, 'SSSSSSSSSSSSSSSSSSSSSSSSSSSS', (SELECT String FROM #CollectMetadata WHERE Section = 'SCHEMA'))
		from #QueryBuilder A
		--update Table Name
		UPDATE A
		SET
			UpdatedQuery = REPLACE(UpdatedQuery, 'XXXXXXXXXXXXXXXXXXXXXXXXXXXX', (SELECT String FROM #CollectMetadata WHERE Section = 'TABLE'))
		from #QueryBuilder A
		--update Insert & Value query
		UPDATE A
		SET
			UpdatedQuery = REPLACE(UpdatedQuery, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAA', (SELECT String FROM #CollectMetadata WHERE Section = 'HEADERS' AND [Order] = 'L01-2'))
		from #QueryBuilder A

		DECLARE @input NVARCHAR(MAX) = (SELECT STRING_AGG(String, ' ') FROM #CollectMetadata WHERE Section = 'DATA');
		SELECT @input = 
			REVERSE(STUFF(REVERSE(@input), CHARINDEX(REVERSE(' UNION ALL '), REVERSE(@input)), LEN(' UNION ALL '), ''))

		--update Data Values
		UPDATE A
		SET
			UpdatedQuery = REPLACE(UpdatedQuery, 'BBBBBBBBBBBBBBBBBBBBBBBBBBBB', LTRIM(@input))
		from #QueryBuilder A


		IF (@result_mode = 0 OR @result_mode = 1)
		BEGIN
			SELECT * FROM #QueryBuilder
		END

		IF (@result_mode = 0 OR @result_mode = 2)
		BEGIN
			SELECT * FROM #CollectMetadata
		END

		IF (@result_mode = 0 OR @result_mode = 3)
		BEGIN
			EXEC('sp_help '''+@owner+'.'+@table_name+'''')
		END

END TRY			

BEGIN CATCH
	DECLARE @msg VARCHAR(255)
	SELECT @msg = (SELECT ERROR_PROCEDURE() + '::' + ERROR_MESSAGE())
	SELECT -1 AS Result, @msg AS LogMessage

	IF @msg IS NOT NULL
	BEGIN
		RAISERROR (@msg, 16, 1);
	END
END CATCH 
END

GO