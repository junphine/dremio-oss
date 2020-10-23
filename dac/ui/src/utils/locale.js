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
import IntlMessageFormat from 'intl-messageformat';
import enStrings from 'dyn-load/locales/en.json';
import zhStrings from 'dyn-load/locales/zh_CN.json';

const messages = {};
messages['en'] = enStrings;
messages['zh-CN'] = zhStrings;

export function getLocale() {
  // todo: write code to actually handle multiple options
  let language = (navigator.languages && navigator.languages[0]) || navigator.language;
  if (!(language in messages)) {
    language = 'en'; // hardcode to only supported option today
  }
  
  let localeStrings = messages[language];
  try {
    if (localStorage.getItem('language')) {
      if (localStorage.getItem('language') === 'ids') {
        //localeStrings = undefined;
        return { language: 'ids', localeStrings: {} };
      } else if (localStorage.getItem('language') === 'double') {
    	localeIdStrings = {}
        for (const [key, value] of Object.entries(localeStrings)) {
          localeIdStrings[key] = key + ' ' + value;
        }
    	return { language: 'double', localeStrings: localeIdStrings };
      } else {
        language = localStorage.getItem('language');
        if (language in messages) {
          localeStrings = messages[language];
        }
        else {
          language = 'en';
          localeStrings = enStrings;
        }
      }
    }
  } catch (e) {
    console.error(e);
  }
  return { language, localeStrings };
}

export function formatMessage(message, values) {
  if(!formatMessage.local)
	  formatMessage.local = getLocale();
  const msg = new IntlMessageFormat(formatMessage.local.localeStrings[message], formatMessage.local.language);
  // todo: write code to actually handle multiple options
  return msg.format(values);
}

export function haveLocKey(key) {
  return key in enStrings;
}

