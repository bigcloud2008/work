package com.ccinfra.spat.domain;

import java.util.List;

/**
 * 相位
 * @author tsing
 *
 */
public class Phase {

	/**信号灯相位ID*/
	private int phaseId;
	/**信号灯相位状态*/
	private List<PhaseState> phaseStates;
	
	public Phase() {
		super();
	}
	
	public Phase(int phaseId, List<PhaseState> phaseStates) {
		super();
		this.phaseId = phaseId;
		this.phaseStates = phaseStates;
	}
	public int getPhaseId() {
		return phaseId;
	}
	public void setPhaseId(int phaseId) {
		this.phaseId = phaseId;
	}
	public List<PhaseState> getPhaseStates() {
		return phaseStates;
	}
	public void setPhaseStates(List<PhaseState> phaseStates) {
		this.phaseStates = phaseStates;
	}
	
}
