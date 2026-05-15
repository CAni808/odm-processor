import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
from io import BytesIO

st.set_page_config(
    page_title="ODM File Processor",
    page_icon="📊",
    layout="wide"
)

MONITORING_COLUMNS = ['SDV', 'Medical Review', 'Data Review']
IGNORED_SITE = 'REDCap Cloud Demo'


def to_yes_no(value):
    if value is None:
        return 'N'

    value_str = str(value).strip().lower()

    if value_str in ['yes', 'true', '1', 'y']:
        return 'Y'

    if value_str in ['no', 'false', '0', 'n']:
        return 'N'

    return 'N'


def is_true_value(value):
    if value is None:
        return False

    value_str = str(value).strip().lower()

    return value_str in [
        'yes',
        'true',
        '1',
        'y',
        'dynamic',
        'created by rule'
    ]


def get_namespace_map(root):
    odm_ns = 'http://www.cdisc.org/ns/odm/v1.3'
    redcap_ns = 'https://www.redcapcloud.com/ns/odm_ext_v132/v10'

    return {
        'odm': odm_ns,
        'REDCap': redcap_ns
    }


def find_elements_once(root, tag_name, namespaces):
    ns_uri = namespaces['odm']

    elements = root.findall(f'.//{{{ns_uri}}}{tag_name}')

    if elements:
        return elements

    return root.findall(f'.//{tag_name}')


def get_redcap_attr(element, attr_name, namespaces):
    ns_uri = namespaces['REDCap']

    value = element.get(f'{{{ns_uri}}}{attr_name}', '')

    if value:
        return value

    return element.get(attr_name, '')


def extract_metadata_versions(root, namespaces):
    metadata_versions = find_elements_once(
        root,
        'MetaDataVersion',
        namespaces
    )

    if not metadata_versions:
        return {}

    site_forms_map = {}

    for mv in metadata_versions:

        # Use LocationOID first
        site_name = mv.get('LocationOID', '')

        # Fallbacks
        if not site_name:
            site_name = mv.get('Name', '')

        if not site_name:
            site_name = mv.get('OID', '')

        if site_name == IGNORED_SITE:
            continue

        form_oids = set()

        form_defs = mv.findall(
            f'.//{{{namespaces["odm"]}}}FormDef'
        )

        for form in form_defs:
            form_oid = form.get('OID', '')

            if form_oid:
                form_oids.add(form_oid)

        form_refs = mv.findall(
            f'.//{{{namespaces["odm"]}}}FormRef'
        )

        for form_ref in form_refs:
            form_oid = form_ref.get('FormOID', '')

            if form_oid:
                form_oids.add(form_oid)

        if site_name in site_forms_map:
            site_forms_map[site_name]['forms'].update(form_oids)
        else:
            site_forms_map[site_name] = {
                'forms': form_oids
            }

    return site_forms_map


def extract_event_definitions(root, namespaces):
    study_event_defs = find_elements_once(
        root,
        'StudyEventDef',
        namespaces
    )

    if not study_event_defs:
        return pd.DataFrame()

    event_definitions = []

    for event in study_event_defs:
        dynamic_event = get_redcap_attr(
            event,
            'DynamicEvent',
            namespaces
        )

        created_by_rule = get_redcap_attr(
            event,
            'CreatedByRule',
            namespaces
        )

        dynamic_created_by_rule = (
            'Y'
            if (
                is_true_value(dynamic_event)
                or is_true_value(created_by_rule)
            )
            else 'N'
        )

        event_info = {
            'Unique Event Name': get_redcap_attr(
                event,
                'UniqueEventName',
                namespaces
            ),
            'Name': event.get('Name', ''),
            'Manual Scheduling': to_yes_no(
                get_redcap_attr(
                    event,
                    'AllowManualSchedule',
                    namespaces
                )
            ),
            'Repeating': to_yes_no(
                event.get('Repeating', '')
            ),
            'Dynamic/Created by Rule': dynamic_created_by_rule
        }

        event_definitions.append(event_info)

    return pd.DataFrame(event_definitions)


def clean_instrument_name(name):
    if not name:
        return name

    dash_pos = name.find(' -')

    if dash_pos > 0:
        return name[:dash_pos].strip()

    return name.strip()


def extract_event_instruments(root, namespaces):
    study_event_defs = find_elements_once(
        root,
        'StudyEventDef',
        namespaces
    )

    form_defs = find_elements_once(
        root,
        'FormDef',
        namespaces
    )

    if not study_event_defs:
        return pd.DataFrame()

    site_forms_map = extract_metadata_versions(
        root,
        namespaces
    )

    all_valid_sites = set(site_forms_map.keys())

    form_oid_to_name = {}

    for form in form_defs:
        oid = form.get('OID', '')

        if oid:
            name = form.get('Name', '')
            name = clean_instrument_name(name)

            form_oid_to_name[oid] = name

    event_instruments = []

    for event in study_event_defs:
        event_name = event.get('Name', '')

        form_refs = event.findall(
            f'.//{{{namespaces["odm"]}}}FormRef'
        )

        for form_ref in form_refs:
            form_oid = form_ref.get('FormOID', '')

            form_sites = set()

            if site_forms_map:
                for site_name, info in site_forms_map.items():
                    if form_oid in info['forms']:
                        form_sites.add(site_name)

            if not form_sites:
                site_display = 'Unknown Site'
            elif form_sites == all_valid_sites:
                site_display = 'All sites'
            else:
                site_display = ', '.join(sorted(form_sites))

            monitoring_types_present = set()

            redcap_ns = namespaces['REDCap']

            monitoring_elems = form_ref.findall(
                f'.//{{{redcap_ns}}}Monitoring'
            )

            for monitoring_elem in monitoring_elems:
                mtype = monitoring_elem.get('Type', '')

                if mtype:
                    monitoring_types_present.add(mtype)

            record = {
                'Event': event_name,
                'Instrument Name': form_oid_to_name.get(
                    form_oid,
                    ''
                ),
                'Version': get_redcap_attr(
                    form_ref,
                    'DefaultVersion',
                    namespaces
                ),
                'Site': site_display,
                'Repeating': to_yes_no(
                    get_redcap_attr(
                        form_ref,
                        'Repeating',
                        namespaces
                    )
                ),
                'Dynamic': to_yes_no(
                    get_redcap_attr(
                        form_ref,
                        'DynamicForm',
                        namespaces
                    )
                ),
                'Required': to_yes_no(
                    form_ref.get('Mandatory', '')
                )
            }

            for col in MONITORING_COLUMNS:
                record[col] = (
                    'Y'
                    if col in monitoring_types_present
                    else 'N'
                )

            event_instruments.append(record)

    df = pd.DataFrame(event_instruments)

    if df.empty:
        return pd.DataFrame()

    final_cols = [
        'Event',
        'Instrument Name',
        'Version',
        'Site',
        'Repeating',
        'Dynamic',
        'Required'
    ] + MONITORING_COLUMNS

    return df[final_cols]


def process_odm_content(xml_content):
    try:
        root = ET.fromstring(xml_content)

        namespaces = get_namespace_map(root)

        df_events = extract_event_definitions(
            root,
            namespaces
        )

        df_instruments = extract_event_instruments(
            root,
            namespaces
        )

        return df_events, df_instruments, None

    except ET.ParseError as e:
        return None, None, f"XML Parse Error: {str(e)}"

    except Exception as e:
        return None, None, f"Error: {str(e)}"


def create_dvs_view(df_events, df_instruments):
    if df_events.empty or df_instruments.empty:
        return pd.DataFrame()

    event_map = {}

    for _, row in df_events.iterrows():
        event_name = row['Name']

        event_map[event_name] = {
            'Unique Event Name': row['Unique Event Name'],
            'Added Manually': row['Manual Scheduling'],
            'Event Repeating': row['Repeating'],
            'Dynamic / Created by Rule': row['Dynamic/Created by Rule']
        }

    dvs_records = []

    for _, inst_row in df_instruments.iterrows():
        event_name = inst_row['Event']

        event_data = event_map.get(
            event_name,
            {
                'Unique Event Name': '',
                'Added Manually': 'N',
                'Event Repeating': 'N',
                'Dynamic / Created by Rule': 'N'
            }
        )

        record = {
            'Unique Event Name': event_data['Unique Event Name'],
            'Event': event_name,
            'Instrument Name': inst_row['Instrument Name'],
            'Version': inst_row['Version'],
            'Site': inst_row['Site'],
            'Repeating': inst_row['Repeating'],
            'Dynamic': inst_row['Dynamic'],
            'Required': inst_row['Required'],
            'Added Manually': event_data['Added Manually'],
            'Event Repeating': event_data['Event Repeating'],
            'Dynamic / Created by Rule': event_data[
                'Dynamic / Created by Rule'
            ],
            'SDV': inst_row['SDV'],
            'Medical Monitoring': inst_row['Medical Review'],
            'Data Review': inst_row['Data Review']
        }

        dvs_records.append(record)

    dvs_df = pd.DataFrame(dvs_records)

    return dvs_df


st.title("📊 ODM File Processor")

st.markdown(
    "Upload ODM XML files to extract ODM event and instrument data."
)

uploaded_file = st.file_uploader(
    "Choose an ODM XML file",
    type=['xml']
)

if uploaded_file is not None:

    st.info(
        f"File: {uploaded_file.name} "
        f"({uploaded_file.size} bytes)"
    )

    xml_content = uploaded_file.read()

    with st.spinner("Processing ODM file..."):
        df_events, df_instruments, error = process_odm_content(
            xml_content
        )

    if error:
        st.error(error)

    else:
        st.success("File processed successfully!")

        df_dvs = create_dvs_view(
            df_events,
            df_instruments
        )

        tab1, tab2, tab3 = st.tabs([
            "Event Definitions",
            "Event Instruments",
            "DVS"
        ])

        with tab1:
            st.dataframe(
                df_events,
                use_container_width=True
            )

        with tab2:
            st.dataframe(
                df_instruments,
                use_container_width=True
            )

        with tab3:
            st.dataframe(
                df_dvs,
                use_container_width=True
            )

        output_filename = uploaded_file.name.replace(
            '.xml',
            '_events.xlsx'
        )

        output = BytesIO()

        with pd.ExcelWriter(
            output,
            engine='openpyxl'
        ) as writer:

            if not df_events.empty:
                df_events.to_excel(
                    writer,
                    sheet_name='Event Definitions',
                    index=False
                )

            if not df_instruments.empty:
                df_instruments.to_excel(
                    writer,
                    sheet_name='Event Instruments',
                    index=False
                )

            if not df_dvs.empty:
                df_dvs.to_excel(
                    writer,
                    sheet_name='DVS',
                    index=False
                )

        output.seek(0)

        st.download_button(
            label="📥 Download Excel File",
            data=output,
            file_name=output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.markdown("---")
st.markdown("ODM File Processor - Web Version")
