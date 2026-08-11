/** dtmrs 子事务屏障 —— 业务服务（RM）侧接入用 */

export const MYSQL: "mysql";
export const POSTGRES: "postgres";
export const SQLITE: "sqlite";

export type Dialect = typeof MYSQL | typeof POSTGRES | typeof SQLITE;

/** 屏障给出的判定 */
export const Decision: {
  /** 该干活。调用方在同一个事务里执行业务 SQL */
  readonly EXECUTE: "EXECUTE";
  /** 空回滚：正向分支从没执行过，补偿空转。**接口应返回成功** */
  readonly NULL_COMPENSATION: "NULL_COMPENSATION";
  /** 重复或悬挂：这次调用已处理过，跳过。**接口应返回成功** */
  readonly DUPLICATED: "DUPLICATED";
};

export type DecisionValue = "EXECUTE" | "NULL_COMPENSATION" | "DUPLICATED";

/** 能执行 SQL 的连接。pg 的 Client / mysql2 的 Connection 都满足 */
export interface Queryable {
  query(sql: string, values?: unknown[]): Promise<unknown>;
}

export class Barrier {
  /**
   * @param op action | compensate | try | confirm | cancel | commit | rollback
   */
  constructor(
    dialect: Dialect,
    transType: string,
    gid: string,
    branchId: string,
    op: string,
    table?: string
  );

  /** 建屏障表。启动时调一次即可，重复调用无害 */
  static migrate(conn: Queryable, dialect: Dialect, table?: string): Promise<void>;

  /**
   * 做出判定。**必须在业务事务里调用**，业务 SQL 要用同一个连接。
   */
  decide(conn: Queryable): Promise<DecisionValue>;
}
