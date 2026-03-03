export type SyncPolicy = {
  schema?: string;
  allowCreateTable?: boolean;
  allowAddColumn?: boolean;
  allowDropColumn?: boolean;
  allowTypeChange?: boolean;
  allowSetNotNull?: boolean;
  allowDropNotNull?: boolean;
};
