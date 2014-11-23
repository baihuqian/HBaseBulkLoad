package edu.cmu.bqian.bulkload;


public enum HColumnEnum {
	COL_B("tweet".getBytes());

	private final byte[] columnName;

	HColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}
