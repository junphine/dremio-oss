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
import { RSAA } from 'redux-api-middleware';
import { push } from 'react-router-redux';
import { APIV2Call } from '@app/core/APICall';
import { saveAsDataset } from 'actions/explore/dataset/save';

import { hideConfirmationDialog, showConfirmationDialog } from 'actions/confirmation';

import FileUtils from '@app/utils/FileUtils';
import config from 'dyn-load/utils/config';
import jobsUtils from 'utils/jobsUtils';
import apiUtils from '@app/utils/apiUtils/apiUtils';

export const START_DATASET_DOWNLOAD = 'START_DATASET_DOWNLOAD';

export const startDownloadDataset = (dataset, format) => {
  return {type: START_DATASET_DOWNLOAD, meta: {dataset, format}};
};

export const downloadDataset = (dataset, format) => (dispatch) => dispatch(fetchDownloadDataset(dataset, format));

export const DOWNLOAD_DATASET_REQUEST = 'DOWNLOAD_DATASET_REQUEST';
export const DOWNLOAD_DATASET_SUCCESS = 'DOWNLOAD_DATASET_SUCCESS';
export const DOWNLOAD_DATASET_FAILURE = 'DOWNLOAD_DATASET_FAILURE';

const fetchDownloadDataset = (dataset, format) => {
  const meta = { dataset, notification: true };
  const href = dataset.getIn(['apiLinks', 'self']) + '/download';

  const apiCall = new APIV2Call()
    .fullpath(href)
    .params({downloadFormat: format});

  return {
    [RSAA]: {
      types: [
        {type: DOWNLOAD_DATASET_REQUEST, meta},
        {type: DOWNLOAD_DATASET_SUCCESS, meta},
        {type: DOWNLOAD_DATASET_FAILURE, meta}
      ],
      method: 'GET',
      headers: {Accept: 'application/json'},
      endpoint: apiCall
    }
  };
};

export const showDownloadModal = (jobId, confirm) => (dispatch) => {
  const jobsHref = jobsUtils.navigationURLForJobId(jobId);
  const onClick = (e) => {
    e.preventDefault();
    confirm(true);
    dispatch(hideConfirmationDialog());
    dispatch(push(jobsHref));
  };
  const currentJobLink = <a href={jobsHref} onClick={onClick}>
    {la('Go to Job')}
  </a>;
  return dispatch(
    showConfirmationDialog({
      title: la('Preparing Download…'),
      confirm: () => confirm(true),
      showOnlyConfirm: true,
      confirmText: la('Dismiss'),
      text: [
        <span>{la('Your download will start when ready.')}</span>,
        currentJobLink
      ]
    })
  );
};

export const OPEN_QLIK_SENSE = 'OPEN_QLIK_SENSE';

function needsSaveBeforeBI(dataset) {
  // if the dataset is a datasetUI and has no apiLinks.namespaceEntity, then we need to save it before doing BI as its
  // an unsaved dataset
  return dataset.get('entityType') === 'datasetUI' && !dataset.getIn(['apiLinks', 'namespaceEntity']);
}

/**
 * Triggers qlik saga
 * @param dataset
 */
export const openQlikSense = (dataset) => {
  return (dispatch) => {
    if (needsSaveBeforeBI(dataset)) {
      return dispatch(saveAsDataset('OPEN_QLIK_AFTER'));
    }

    return dispatch({
      type: OPEN_QLIK_SENSE,
      payload: dataset
    });
  };
};

export const LOAD_QLIK_APP_START   = 'LOAD_QLIK_APP_START';
export const LOAD_QLIK_APP_SUCCESS = 'LOAD_QLIK_APP_SUCCESS';
export const LOAD_QLIK_APP_FAILURE = 'LOAD_QLIK_APP_FAILURE';


export const OPEN_DAVINCI = 'OPEN_DAVINCI';
/**
 * Triggers davinci saga
 * @param dataset
 */
export const openDavinci = (dataset) => {
  return (dispatch) => {
    if (needsSaveBeforeBI(dataset)) {
      return dispatch(saveAsDataset('OPEN_DAVINCI_AFTER'));
    }

    const href = `/davinci/${FileUtils.getDatasetIdForClientTools(dataset)}`;
    return dispatch(downloadPowerBI({ href }))
      .then((response) => {
        if (!response.error) {          
          //FileUtils.downloadFile(response.payload);
          const reader = new FileReader();
          reader.readAsText(response.payload.blob, 'utf-8');
          reader.onload = function (e) {
        	console.info(reader.result);
        	const payload = JSON.parse(reader.result);
        	const address = payload.connections[0].details.address;
            const schema = address.schema;
            const table = address.object;                     
            const href = config.davinciUrl + '?q=dremio/'+ address.server+'/'+schema+'/'+table;
            //const post = apiUtils.fetch(href);
            //const w = window.open(href); 
            
            const currentJobLink = <a href={href} target="blank">{la('Go to Vaninci BI Tools')}</a>;
	        return dispatch(
	            showConfirmationDialog({
	              title: la('Preparing BI Tools…'),
	              confirm: () => window.open(href),
	              showOnlyConfirm: false,
	              confirmText: la('Go'),
	              text: [
	                <span>{la('Your will go Vaninci web BI tool when ready.')}</span>,
	                currentJobLink
	              ]
	            })
	           );
          }
          
        }
        else{
        	
        }
      });
  };
};

export const downloadTableau = ({ href }) => (dispatch) => dispatch(fetchDownloadTableau({ href }));

export const LOAD_TABLEAU_START   = 'LOAD_TABLEAU_START';
export const LOAD_TABLEAU_SUCCESS = 'LOAD_TABLEAU_SUCCESS';
export const LOAD_TABLEAU_FAILURE = 'LOAD_TABLEAU_FAILURE';

const fetchDownloadTableau = ({ href }) => {
  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        LOAD_TABLEAU_START,
        {type: LOAD_TABLEAU_SUCCESS, payload: (action, state, res) => FileUtils.getFileDownloadConfigFromResponse(res)},
        {
          type: LOAD_TABLEAU_FAILURE,
          meta: {
            notification: {
              message: la('There was an error preparing for Tableau.'),
              level: 'error'
            }
          }
        }
      ],
      headers: {Accept: config.tdsMimeType},
      method: 'GET',
      endpoint: apiCall
    }
  };
};

export const openTableau = (dataset) => {
  return (dispatch) => {
    if (needsSaveBeforeBI(dataset)) {
      return dispatch(saveAsDataset('OPEN_TABLEAU'));
    }

    const href = `/tableau/${FileUtils.getDatasetIdForClientTools(dataset)}`;
    return dispatch(downloadTableau({ href }))
      .then((response) => {
        if (!response.error) {
          FileUtils.downloadFile(response.payload);
        }
      });
  };
};


export const downloadPowerBI = ({ href }) => (dispatch) => dispatch(fetchDownloadPowerBI({ href }));
export const LOAD_POWER_BI_START = 'LOAD_POWER_BI_START';
export const LOAD_POWER_BI_SUCCESS = 'LOAD_POWER_BI_SUCCESS';
export const LOAD_POWER_BI_FAILURE = 'LOAD_POWER_BI_FAILURE';

const fetchDownloadPowerBI = ({ href }) => {
  const apiCall = new APIV2Call().fullpath(href);

  return {
    [RSAA]: {
      types: [
        LOAD_POWER_BI_START,
        {type: LOAD_POWER_BI_SUCCESS, payload: (action, state, res) => FileUtils.getFileDownloadConfigFromResponse(res)},
        {
          type: LOAD_POWER_BI_FAILURE,
          meta: {
            notification: {
              message: la('There was an error preparing for Power BI.'),
              level: 'error'
            }
          }
        }
      ],
      headers: {Accept: 'application/pbids'},
      method: 'GET',
      endpoint: apiCall
    }
  };
};


export const openPowerBI = (dataset) => {
  return (dispatch) => {
    if (needsSaveBeforeBI(dataset)) {
      return dispatch(saveAsDataset('OPEN_POWER_BI'));
    }

    const href = `/powerbi/${FileUtils.getDatasetIdForClientTools(dataset)}`;
    return dispatch(downloadPowerBI({ href }))
      .then((response) => {
        if (!response.error) {
          FileUtils.downloadFile(response.payload);
        }
      });
  };
};
