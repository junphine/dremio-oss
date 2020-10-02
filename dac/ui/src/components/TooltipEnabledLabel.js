/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Component } from 'react';
import pureRender from 'pure-render-decorator';
import PropTypes, { oneOfType } from 'prop-types';
import classNames from 'classnames';
import { Tooltip } from 'components/Tooltip';

@pureRender
export default class TooltipEnabledLabel extends Component {
  static propTypes = {
    label: PropTypes.node,
    labelContentClass: PropTypes.string,
    style: PropTypes.object,
    children: PropTypes.arrayOf(PropTypes.any),
    className: oneOfType([PropTypes.string, PropTypes.arrayOf(PropTypes.string)]),
    labelBefore: PropTypes.bool,
    tooltipStyle: PropTypes.object,
    tooltipInnerStyle: PropTypes.object,
    tooltip: PropTypes.node,
    toolTipPosition: PropTypes.string
  }

  constructor(props) {
    super(props);
    this.state = {hover: false};
    this.onMouseEnter = this.onMouseEnter.bind(this);
    this.onMouseLeave = this.onMouseLeave.bind(this);
  }

  onMouseEnter() {
    this.setState({hover: true});
  }

  onMouseLeave() {
    this.setState({hover: false});
  }

  renderLabel() {
    const {labelContentClass, label, tooltip} = this.props;
    const tooltipProps = tooltip ? {
      ref: 'target',
      onMouseEnter: this.onMouseEnter,
      onMouseLeave: this.onMouseLeave
    } : {};
    return <span
      className={labelContentClass}
      onMouseEnter={this.onMouseEnter}
      onMouseLeave={this.onMouseLeave}
      {...tooltipProps}>{label}
    </span>;
  }

  isLabelBefore() {
    return this.props.labelBefore;
  }

  render() {
    const {className, style, toolTipPosition, tooltipStyle, tooltipInnerStyle, tooltip} = this.props;
    const {hover} = this.state;
    const finalInnerStyle = {...styles.defaultInnerStyle, ...tooltipInnerStyle};
    return (
      <label className={classNames(className)} key='container' style={style}>
        {this.isLabelBefore() && this.renderLabel()}
        {this.props.children}
        {!this.isLabelBefore() && this.renderLabel()}
        {tooltip &&
          <Tooltip
            container={this}
            placement={toolTipPosition || 'bottom-start'}
            target={() => hover ? this.refs.target : null}
            type='status'
            style={tooltipStyle}
            tooltipInnerStyle={finalInnerStyle}
          >
            {tooltip}
          </Tooltip>
        }
      </label>
    );
  }
}

const styles = {
  defaultInnerStyle: {
    borderRadius: 5,
    padding: 10,
    width: 300
  }
};
