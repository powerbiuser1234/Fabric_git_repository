[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data]{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "BACKDATED_NOSTRO_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CHAPTER_ID", DestinationColumnName = "CHAPTER_ID"], [SourceColumnName = "BRANCH_ID", DestinationColumnName = "BRANCH_ID"], [SourceColumnName = "ACCOUNT_ID", DestinationColumnName = "ACCOUNT_ID"], [SourceColumnName = "CURRENCY", DestinationColumnName = "CURRENCY"], [SourceColumnName = "NAME_OF_THE_ACCOUNT", DestinationColumnName = "NAME_OF_THE_ACCOUNT"], [SourceColumnName = "PIECE_NO", DestinationColumnName = "PIECE_NO"], [SourceColumnName = "DATE_COMPT", DestinationColumnName = "DATE_COMPT"], [SourceColumnName = "OPE_ID", DestinationColumnName = "OPE_ID"], [SourceColumnName = "NARRATION", DestinationColumnName = "NARRATION"], [SourceColumnName = "SEN", DestinationColumnName = "SEN"], [SourceColumnName = "AMOUNT", DestinationColumnName = "AMOUNT"], [SourceColumnName = "SWIFT_REFNO", DestinationColumnName = "SWIFT_REFNO"], [SourceColumnName = "MAKER_NAME", DestinationColumnName = "MAKER_NAME"], [SourceColumnName = "CHECKER_NAME", DestinationColumnName = "CHECKER_NAME"], [SourceColumnName = "CURRENCY_NOST", DestinationColumnName = "CURRENCY_NOST"], [SourceColumnName = "NOSTRO_ACCOUNT_NO", DestinationColumnName = "NOSTRO_ACCOUNT_NO"], [SourceColumnName = "ACC_NOSTRO_RB", DestinationColumnName = "ACC_NOSTRO_RB"], [SourceColumnName = "DAG", DestinationColumnName = "DAG"], [SourceColumnName = "HIS_EVE", DestinationColumnName = "HIS_EVE"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared BACKDATED_NOSTRO = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "BACKDATED_NOSTRO"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "CORRESPONDANTACCOUNT_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ACCOUNT_DEV", DestinationColumnName = "ACCOUNT_DEV"], [SourceColumnName = "CORRES_BANK_NAME", DestinationColumnName = "CORRES_BANK_NAME"], [SourceColumnName = "ACCOUNT_NO", DestinationColumnName = "ACCOUNT_NO"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared CORRESPONDANTACCOUNT = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "CORRESPONDANTACCOUNT"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "MONITOR_NOSTRO_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "DATES", DestinationColumnName = "DATES"], [SourceColumnName = "STEP", DestinationColumnName = "STEP"], [SourceColumnName = "TIME", DestinationColumnName = "TIME"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared MONITOR_NOSTRO = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "MONITOR_NOSTRO"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CHAPTER_ID", DestinationColumnName = "CHAPTER_ID"], [SourceColumnName = "BRANCH_ID", DestinationColumnName = "BRANCH_ID"], [SourceColumnName = "ACCOUNT_ID", DestinationColumnName = "ACCOUNT_ID"], [SourceColumnName = "CURRENCY", DestinationColumnName = "CURRENCY"], [SourceColumnName = "NAME_OF_THE_ACCOUNT", DestinationColumnName = "NAME_OF_THE_ACCOUNT"], [SourceColumnName = "PIECE_NO", DestinationColumnName = "PIECE_NO"], [SourceColumnName = "DATE_COMPT", DestinationColumnName = "DATE_COMPT"], [SourceColumnName = "OPE_ID", DestinationColumnName = "OPE_ID"], [SourceColumnName = "NARRATION", DestinationColumnName = "NARRATION"], [SourceColumnName = "SEN", DestinationColumnName = "SEN"], [SourceColumnName = "AMOUNT", DestinationColumnName = "AMOUNT"], [SourceColumnName = "SWIFT_REFNO", DestinationColumnName = "SWIFT_REFNO"], [SourceColumnName = "MAKER_NAME", DestinationColumnName = "MAKER_NAME"], [SourceColumnName = "CHECKER_NAME", DestinationColumnName = "CHECKER_NAME"], [SourceColumnName = "CURRENCY_NOST", DestinationColumnName = "CURRENCY_NOST"], [SourceColumnName = "NOSTRO_ACCOUNT_NO", DestinationColumnName = "NOSTRO_ACCOUNT_NO"], [SourceColumnName = "ACC_NOSTRO_RB", DestinationColumnName = "ACC_NOSTRO_RB"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO_2_3_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CHAPTER_ID", DestinationColumnName = "CHAPTER_ID"], [SourceColumnName = "BRANCH_ID", DestinationColumnName = "BRANCH_ID"], [SourceColumnName = "ACCOUNT_ID", DestinationColumnName = "ACCOUNT_ID"], [SourceColumnName = "CURRENCY", DestinationColumnName = "CURRENCY"], [SourceColumnName = "NAME_OF_THE_ACCOUNT", DestinationColumnName = "NAME_OF_THE_ACCOUNT"], [SourceColumnName = "PIECE_NO", DestinationColumnName = "PIECE_NO"], [SourceColumnName = "DATE_COMPT", DestinationColumnName = "DATE_COMPT"], [SourceColumnName = "OPE_ID", DestinationColumnName = "OPE_ID"], [SourceColumnName = "NARRATION", DestinationColumnName = "NARRATION"], [SourceColumnName = "SEN", DestinationColumnName = "SEN"], [SourceColumnName = "AMOUNT", DestinationColumnName = "AMOUNT"], [SourceColumnName = "SWIFT_REFNO", DestinationColumnName = "SWIFT_REFNO"], [SourceColumnName = "MAKER_NAME", DestinationColumnName = "MAKER_NAME"], [SourceColumnName = "CHECKER_NAME", DestinationColumnName = "CHECKER_NAME"], [SourceColumnName = "CURRENCY_NOST", DestinationColumnName = "CURRENCY_NOST"], [SourceColumnName = "NOSTRO_ACCOUNT_NO", DestinationColumnName = "NOSTRO_ACCOUNT_NO"], [SourceColumnName = "ACC_NOSTRO_RB", DestinationColumnName = "ACC_NOSTRO_RB"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO_2_3 = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO_2_3"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO_BAL_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "AGE", DestinationColumnName = "AGE"], [SourceColumnName = "ACCOUNT", DestinationColumnName = "ACCOUNT"], [SourceColumnName = "DEV", DestinationColumnName = "DEV"], [SourceColumnName = "NAME", DestinationColumnName = "NAME"], [SourceColumnName = "BALANCE", DestinationColumnName = "BALANCE"], [SourceColumnName = "ASONDATE", DestinationColumnName = "ASONDATE"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO_BAL = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO_BAL"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO_MONITOR_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "DATES", DestinationColumnName = "DATES"], [SourceColumnName = "STEP", DestinationColumnName = "STEP"], [SourceColumnName = "TIME", DestinationColumnName = "TIME"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO_MONITOR = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO_MONITOR"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO_PART1_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CHAPTER_ID", DestinationColumnName = "CHAPTER_ID"], [SourceColumnName = "BRANCH_ID", DestinationColumnName = "BRANCH_ID"], [SourceColumnName = "ACCOUNT_ID", DestinationColumnName = "ACCOUNT_ID"], [SourceColumnName = "CURRENCY", DestinationColumnName = "CURRENCY"], [SourceColumnName = "NAME_OF_THE_ACCOUNT", DestinationColumnName = "NAME_OF_THE_ACCOUNT"], [SourceColumnName = "PIECE_NO", DestinationColumnName = "PIECE_NO"], [SourceColumnName = "DATE_COMPT", DestinationColumnName = "DATE_COMPT"], [SourceColumnName = "OPE_ID", DestinationColumnName = "OPE_ID"], [SourceColumnName = "NARRATION", DestinationColumnName = "NARRATION"], [SourceColumnName = "SEN", DestinationColumnName = "SEN"], [SourceColumnName = "AMOUNT", DestinationColumnName = "AMOUNT"], [SourceColumnName = "SWIFT_REFNO", DestinationColumnName = "SWIFT_REFNO"], [SourceColumnName = "MAKER_NAME", DestinationColumnName = "MAKER_NAME"], [SourceColumnName = "CHECKER_NAME", DestinationColumnName = "CHECKER_NAME"], [SourceColumnName = "CURRENCY_NOST", DestinationColumnName = "CURRENCY_NOST"], [SourceColumnName = "NOSTRO_ACCOUNT_NO", DestinationColumnName = "NOSTRO_ACCOUNT_NO"], [SourceColumnName = "ACC_NOSTRO_RB", DestinationColumnName = "ACC_NOSTRO_RB"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO_PART1 = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO_PART1"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO_PART2_3_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CHAPTER_ID", DestinationColumnName = "CHAPTER_ID"], [SourceColumnName = "BRANCH_ID", DestinationColumnName = "BRANCH_ID"], [SourceColumnName = "ACCOUNT_ID", DestinationColumnName = "ACCOUNT_ID"], [SourceColumnName = "CURRENCY", DestinationColumnName = "CURRENCY"], [SourceColumnName = "NAME_OF_THE_ACCOUNT", DestinationColumnName = "NAME_OF_THE_ACCOUNT"], [SourceColumnName = "PIECE_NO", DestinationColumnName = "PIECE_NO"], [SourceColumnName = "DATE_COMPT", DestinationColumnName = "DATE_COMPT"], [SourceColumnName = "OPE_ID", DestinationColumnName = "OPE_ID"], [SourceColumnName = "NARRATION", DestinationColumnName = "NARRATION"], [SourceColumnName = "SEN", DestinationColumnName = "SEN"], [SourceColumnName = "AMOUNT", DestinationColumnName = "AMOUNT"], [SourceColumnName = "SWIFT_REFNO", DestinationColumnName = "SWIFT_REFNO"], [SourceColumnName = "MAKER_NAME", DestinationColumnName = "MAKER_NAME"], [SourceColumnName = "CHECKER_NAME", DestinationColumnName = "CHECKER_NAME"], [SourceColumnName = "CURRENCY_NOST", DestinationColumnName = "CURRENCY_NOST"], [SourceColumnName = "NOSTRO_ACCOUNT_NO", DestinationColumnName = "NOSTRO_ACCOUNT_NO"], [SourceColumnName = "ACC_NOSTRO_RB", DestinationColumnName = "ACC_NOSTRO_RB"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO_PART2_3 = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO_PART2_3"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "NOSTRO1_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "CHAPTER_ID", DestinationColumnName = "CHAPTER_ID"], [SourceColumnName = "BRANCH_ID", DestinationColumnName = "BRANCH_ID"], [SourceColumnName = "ACCOUNT_ID", DestinationColumnName = "ACCOUNT_ID"], [SourceColumnName = "CURRENCY", DestinationColumnName = "CURRENCY"], [SourceColumnName = "NAME_OF_THE_ACCOUNT", DestinationColumnName = "NAME_OF_THE_ACCOUNT"], [SourceColumnName = "PIECE_NO", DestinationColumnName = "PIECE_NO"], [SourceColumnName = "DATE_COMPT", DestinationColumnName = "DATE_COMPT"], [SourceColumnName = "OPE_ID", DestinationColumnName = "OPE_ID"], [SourceColumnName = "NARRATION", DestinationColumnName = "NARRATION"], [SourceColumnName = "SEN", DestinationColumnName = "SEN"], [SourceColumnName = "AMOUNT", DestinationColumnName = "AMOUNT"], [SourceColumnName = "SWIFT_REFNO", DestinationColumnName = "SWIFT_REFNO"], [SourceColumnName = "MAKER_NAME", DestinationColumnName = "MAKER_NAME"], [SourceColumnName = "CHECKER_NAME", DestinationColumnName = "CHECKER_NAME"], [SourceColumnName = "CURRENCY_NOST", DestinationColumnName = "CURRENCY_NOST"], [SourceColumnName = "NOSTRO_ACCOUNT_NO", DestinationColumnName = "NOSTRO_ACCOUNT_NO"], [SourceColumnName = "ACC_NOSTRO_RB", DestinationColumnName = "ACC_NOSTRO_RB"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared NOSTRO1 = let
  Source = Oracle.Database("192.168.1.50:1521/rbkbidb", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "CALLCENTER"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "NOSTRO1"]}[Data]
in
  #"Navigation 2";
shared BACKDATED_NOSTRO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "BACKDATED_NOSTRO", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared CORRESPONDANTACCOUNT_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "CORRESPONDANTACCOUNT", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared MONITOR_NOSTRO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "MONITOR_NOSTRO", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO_2_3_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO_2_3", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO_BAL_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO_BAL", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO_MONITOR_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO_MONITOR", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO_PART1_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO_PART1", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO_PART2_3_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO_PART2_3", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared NOSTRO1_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "e10c1eb4-131b-4138-b4ac-b8e97bb56785"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "aec0f587-2c19-4577-95d0-800394d4b9b9"]}[Data],
  TableNavigation = Navigation_2{[Item = "NOSTRO1", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
