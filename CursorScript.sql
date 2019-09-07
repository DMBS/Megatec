--Скрипт для изменения длины поля TU_FNAMERUS

--объявляем переменные
declare @table_name sysname
declare @index_name sysname
declare @schema_name sysname
declare @index_column sysname
declare @include_columns sysname
declare @dynamic_collation sql_variant
declare @column_name NVARCHAR(35) = 'TU_FNAMERUS'

create table #temporary (

	tmp_table_name sysname NULL,
	tmp_index_name sysname NULL,
	tmp_schema_name sysname NULL,
	tmp_index_column sysname NULL,
	tmp_include_columns sysname NULL

);

declare @index_information table (
	table_name sysname NULL,
	index_name sysname NULL,
	[schema_name] sysname NULL,
	index_column sysname NULL,
	include_columns sysname NULL
);

declare @dropindex NVARCHAR(MAX)
declare @recreateindex NVARCHAR(MAX)
declare @alter_column_lenght NVARCHAR (MAX)

-- записываем найденные в системе индексы с включенными полями в @temp_table

insert into @index_information (table_name, index_name, [schema_name], index_column, include_columns)
select 
'[' + lower(object_name(si.object_id)) + ']' as table_name,
'[' + lower(si.name) + ']' as index_name,
'[' + lower(s.name) + ']' as [schema_name],

stuff((
	select ',' + '[' + col_name(ic.object_id,ic.column_id) +']' from sys.index_columns ic
	where ic.object_id = si.object_id and ic.index_id = si.index_id and ic.is_included_column = 0
	order by ic.key_ordinal
	for xml path('')) ,1,1,'') as 'index_columns',
stuff((
	select ',' + '[' + col_name(ic.object_id,ic.column_id) + ']' from sys.index_columns ic
	where ic.object_id = si.object_id and ic.index_id = si.index_id	and ic.is_included_column = 1
	for xml path('')) ,1,1,'') as 'include_columns'
 from sys.indexes si
	inner join sys.index_columns sic on si.object_id = sic.object_id and si.index_id = sic.index_id
	inner join sys.columns c on sic.column_id = c.column_id and sic.object_id = c.object_id
	inner join sys.objects so on si.object_id = so.object_id
	inner join sys.tables t ON t.object_id = si.object_id
    inner join sys.schemas s ON t.schema_id = s.schema_id
	left outer join sys.sysconstraints sc on so.object_id = sc.id
where si.type_desc <> 'heap' and si.name <> 'sysdiagrams'
and so.type = 'u'
and lower(object_name(si.object_id)) = 'tbl_Turist'
and (c.name LIKE '%TU_FNAMERUS%' )
group by si.name,si.object_id,s.name, si.index_id,si.is_primary_key,si.type,si.is_unique,si.is_unique_constraint
order by si.type,si.name

--Записываем во временную таблицу информацию о существующих индексах, для дальнейшего их пересоздания
insert into #temporary (tmp_table_name, tmp_index_name, tmp_schema_name, tmp_index_column, tmp_include_columns)
select * from @index_information

--получаем текущий collation SQL сервера
set @dynamic_collation = (select serverproperty('collation'))

--Обьявляем курсор и проходим по всем найденным индексам.
--1)Удаляем индексы
declare index_cursor cursor local fast_forward
for
select * from @index_information

-- открываем курсор
open index_cursor

-- получаем первый индекс 
fetch next from index_cursor 
into @table_name, @index_name, @schema_name, @index_column, @include_columns

while @@fetch_status = 0
begin

--удаляем индекс
	begin
		set @dropindex = N'DROP INDEX ' + (@index_name) + 
					     N' ON ' + (@schema_name) + N'.' + (@table_name)
		print @dropindex
		exec sp_executesql @dropindex
	end

--переходим к следующей строчке
fetch next from index_cursor 
into @table_name, @index_name, @schema_name, @index_column, @include_columns
end

--закрываем курсор
close index_cursor
deallocate index_cursor

--изменяем таблицу и меняем размер длины поля c использованем динамического Collation
set @alter_column_lenght = N'IF EXISTS (SELECT * FROM dbo.syscolumns WHERE name = ' + QUOTENAME(@column_name, '''') + 
						   N' AND id = object_id(' + QUOTENAME('[dbo].[tbl_Turist]', '''') + 
						   N'))
						   BEGIN
					       ALTER TABLE [dbo].[tbl_Turist] ALTER COLUMN ' + (@column_name) + 
						   N' VARCHAR(35) COLLATE Cyrillic_General_CS_AS NOT NULL
						   END'
						   --N' VARCHAR(35) COLLATE ' + CAST(@dynamic_collation as NVARCHAR(max)) + N' NOT NULL

print @alter_column_lenght
exec sp_executesql @alter_column_lenght 

--пересоздаем индексы вместе с include columns, если присутствуют
--объявляем курсор, запрашиваем данные из временной таблицы 
declare index_recreate_cursor cursor local fast_forward
for
select * from #temporary

-- открываем курсор
open index_recreate_cursor

-- получаем первый индекс 
fetch next from index_recreate_cursor
into @table_name, @index_name, @schema_name, @index_column, @include_columns

--проходим циклом по существующим индексам во временной таблице и пересоздаем индексы.
--если индекс имеет include_columns используем конструкцию с With Include
while @@fetch_status = 0
	begin
		if @include_columns is null
			begin
				set @recreateindex = N'CREATE INDEX ' + (@index_name) +
								     N' ON ' + (@schema_name) + N'.' + 
							         (@table_name) + N'(' + (@index_column) + N') ' +
							         N'WITH(PAD_INDEX = OFF,
							         STATISTICS_NORECOMPUTE = OFF,
							         SORT_IN_TEMPDB = OFF,
							         DROP_EXISTING = OFF,
							         ONLINE = OFF,
							         ALLOW_ROW_LOCKS = ON,
							         ALLOW_PAGE_LOCKS = ON,
							         FILLFACTOR = 95) ON [PRIMARY] '
				print @recreateindex
				exec sp_executesql @recreateindex
			end
		else
			begin
				set @recreateindex = N'CREATE INDEX ' + (@index_name) +
					                 N' ON ' + (@schema_name) + N'.' + 
							         (@table_name) + N'(' + (@index_column) + N') ' +
							         N'INCLUDE ' + N'(' + (@include_columns) + N') ' +
							         N'WITH(PAD_INDEX = OFF,
							         STATISTICS_NORECOMPUTE = OFF,
							         SORT_IN_TEMPDB = OFF,
							         DROP_EXISTING = OFF,
							         ONLINE = OFF,
							         ALLOW_ROW_LOCKS = ON,
							         ALLOW_PAGE_LOCKS = ON,
							         FILLFACTOR = 95) ON [PRIMARY] '
				print @recreateindex
				exec sp_executesql @recreateindex
			end

--переходим к следующему индексу
fetch next from index_recreate_cursor
into @table_name, @index_name, @schema_name, @index_column, @include_columns
end

--закрываем курсор
close index_recreate_cursor
deallocate index_recreate_cursor
--удаляем временную таблицу
drop table #temporary
GO