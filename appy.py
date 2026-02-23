import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
from io import BytesIO

st.set_page_config(page_title="ODM File Processor", page_icon="📊", layout="wide")

MONITORING_COLUMNS = ['SDV', 'Medical Review', 'Data Review']
IGNORED_SITE = 'REDCap Cloud Demo'

def to_yes_no(value):
    if value is None:
        return 'N'
    value_str = str(value).strip().lower()
    if value_str in ['yes', 'true', '1', 'y']:
        return 'Y'
    elif value_str in ['no', 'false', '0', 'n']:
        return 'N'
    else:
        return 'N' if not value_str else value_str.upper()

def is_true_value(value):
    if value is None:
        return False
    value_str = str(value).strip().lower()
    return value_str in ['yes', 'true', '1', 'y', 'dynamic', 'created by rule']

def get_namespace_map(root):
    odm_ns = 'http://www.cdisc.org/ns/odm/v1.3'
    redcap_ns = 'https://www.redcapcloud.com/ns/odm_ext_v132/v10'

    if '}' in root.tag:
        ns_uri = root.tag.split('}')[0][1:]
        if 'odm' in ns_uri.lower():
            odm_ns = ns_uri

    for attr, value in root.attrib.items():
        if 'xmlns' in attr:
            if 'odm' in value.lower() and 'cdisc' in value.lower():
                odm_ns = value
            if 'redcap' in value.lower():
                redcap_ns = value

    return {'odm': odm_ns, 'REDCap': redcap_ns}

def find_elements_once(root, tag_name, namespaces):
    ns_uri = namespaces.get('odm', 'http://www.cdisc.org/ns/odm/v1.3')
    elements = root.findall(f'.//{{{ns_uri}}}{tag_name}')
    if elements:
        return elements
    elements = root.findall(f'.//odm:{tag_name}', namespaces)
    if elements:
        return elements
    return root.findall(f'.//{tag_name}')

def get_redcap_attr(element, attr_name, namespaces):
    ns_uri = namespaces.get('REDCap', 'https://www.redcapcloud.com/ns/odm_ext_v132/v10')
    value = element.get(f'{{{ns_uri}}}{attr_name}', '')
    if value:
        return value
    return element.get(attr_name, '')

def extract_metadata_versions(root, namespaces):
    metadata_versions = find_elements_once(root, 'MetaDataVersion', namespaces)
    if not metadata_versions:
        return {}
    
    site_forms_map = {}
    for mv in metadata_versions:
        mv_oid = mv.get('OID', '')
        site_name = mv.get('Name', '')
        if not site_name:
            site_name = mv_oid
        if site_name == IGNORED_SITE:
            continue
        
        form_oids = set()
        form_defs = mv.findall(f'.//{{{namespaces.get("odm", "http://www.cdisc.org/ns/odm/v1.3")}}}FormDef')
        if not form_defs:
            form_defs = mv.findall('.//odm:FormDef', namespaces)
        if not form_defs:
            form_defs = mv.findall('.//FormDef')
        for form in form_defs:
            form_oid = form.get('OID', '')
            if form_oid:
                form_oids.add(form_oid)
        
        form_refs = mv.findall(f'.//{{{namespaces.get("odm", "http://www.cdisc.org/ns/odm/v1.3")}}}FormRef')
        if not form_refs:
            form_refs = mv.findall('.//odm:FormRef', namespaces)
        if not form_refs:
            form_refs = mv.findall('.//FormRef')
        for form_ref in form_refs:
            form_oid = form_ref.get('FormOID', '')
            if form_oid:
                form_oids.add(form_oid)
        
        if site_name in site_forms_map:
            site_forms_map[site_name]['forms'].update(form_oids)
        else:
            site_forms_map[site_name] = {'oid': mv_oid, 'forms': form_oids}
    return site_forms_map

def extract_event_definitions(root, namespaces):
    study_event_defs = find_elements_once(root, 'StudyEventDef', namespaces)
    if not study_event_defs:
        return pd.DataFrame()

    event_definitions = []
    seen_oids = set()
    for event in study_event_defs:
        oid = event.get('OID', '')
        if not oid or oid in seen_oids:
            continue
        seen_oids.add(oid)

        dynamic_event = get_redcap_attr(event, 'DynamicEvent', namespaces)
        created_by_rule = get_redcap_attr(event, 'CreatedByRule', namespaces)
        dynamic_created_by_rule = 'Y' if (is_true_value(dynamic_event) or is_true_value(created_by_rule)) else 'N'

        event_info = {
            'Unique Event Name': get_redcap_attr(event, 'UniqueEventName', namespaces),
            'Name': event.get('Name', ''),
            'Manual Scheduling': to_yes_no(get_redcap_attr(event, 'AllowManualSchedule', namespaces)),
            'Repeating': to_yes_no(event.get('Repeating', '')),
            'Dynamic/Created by Rule': dynamic_created_by_rule
        }
        event_definitions.append(event_info)
    return pd.DataFrame(event_definitions)

def extract_event_instruments(root, namespaces):
    study_event_defs = find_elements_once(root, 'StudyEventDef', namespaces)
    form_defs = find_elements_once(root, 'FormDef', namespaces)
    if not study_event_defs:
        return pd.DataFrame()

    site_forms_map = extract_metadata_versions(root, namespaces)
    all_valid_sites = set(site_forms_map.keys())

    form_oid_to_name = {}
    seen_form_oids = set()
    for form in form_defs:
        oid = form.get('OID', '')
        if oid and oid not in seen_form_oids:
            seen_form_oids.add(oid)
            form_oid_to_name[oid] = form.get('Name', '')

    event_instruments = []
    seen_event_form_combos = set()

    for event in study_event_defs:
        event_oid = event.get('OID', '')
        event_name = event.get('Name', '')

        form_refs = []
        redcap_ns = namespaces.get('REDCap')
        if namespaces.get('odm'):
            form_refs = event.findall(f'odm:FormRef', namespaces)
        if not form_refs and redcap_ns:
            form_refs = event.findall(f'FormRef')
        if not form_refs:
            for child in event:
                if 'FormRef' in child.tag or child.tag.endswith('FormRef'):
                    form_refs.append(child)

        for form_ref in form_refs:
            form_oid = form_ref.get('FormOID', '')
            combo_key = (event_oid, form_oid)
            if not form_oid or combo_key in seen_event_form_combos:
                continue
            seen_event_form_combos.add(combo_key)

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
            if redcap_ns:
                monitoring_elems = form_ref.findall(f'.//{{{redcap_ns}}}Monitoring')
                if not monitoring_elems:
                    monitoring_elems = form_ref.findall('.//REDCap:Monitoring', namespaces)
                if not monitoring_elems:
                    for child in form_ref:
                        if 'Monitoring' in child.tag:
                            monitoring_elems.append(child)
                for monitoring_elem in monitoring_elems:
                    mtype = monitoring_elem.get('Type', '')
                    if mtype:
                        monitoring_types_present.add(mtype)

            record = {
                'Event': event_name,
                'Instrument Name': form_oid_to_name.get(form_oid, ''),
                'Version': get_redcap_attr(form_ref, 'DefaultVersion', namespaces),
                'Site': site_display,
                'Repeating': to_yes_no(get_redcap_attr(form_ref, 'Repeating', namespaces)),
                'Dynamic': to_yes_no(get_redcap_attr(form_ref, 'DynamicForm', namespaces)),
                'Required': to_yes_no(form_ref.get('Mandatory', ''))
            }
            for col in MONITORING_COLUMNS:
                record[col] = 'Y' if col in monitoring_types_present else 'N'
            event_instruments.append(record)

    df = pd.DataFrame(event_instruments)
    if df.empty:
        columns = ['Event', 'Instrument Name', 'Version', 'Site', 'Repeating', 'Dynamic', 'Required'] + MONITORING_COLUMNS
        return pd.DataFrame(columns=columns)
    final_cols = ['Event', 'Instrument Name', 'Version', 'Site', 'Repeating', 'Dynamic', 'Required'] + MONITORING_COLUMNS
    final_cols = [c for c in final_cols if c in df.columns]
    return df[final_cols]

def process_odm_content(xml_content):
    try:
        root = ET.fromstring(xml_content)
        namespaces = get_namespace_map(root)
        df_events = extract_event_definitions(root, namespaces)
        df_instruments = extract_event_instruments(root, namespaces)
        return df_events, df_instruments, None
    except ET.ParseError as e:
        return None, None, f"XML Parse Error: {str(e)}"
    except Exception as e:
        return None, None, f"Error: {str(e)}"

# ============================================================================
# STREAMLIT UI
# ============================================================================

st.title("📊 ODM File Processor")
st.markdown("Upload ODM XML files to extract Event Definitions and Event Instruments.")

uploaded_file = st.file_uploader("Choose an ODM XML file", type=['xml'])

if uploaded_file is not None:
    st.info(f"**File:** {uploaded_file.name} ({uploaded_file.size} bytes)")
    xml_content = uploaded_file.read()
    
    with st.spinner("Processing ODM file..."):
        df_events, df_instruments, error = process_odm_content(xml_content)
    
    if error:
        st.error(error)
    else:
        st.success("✓ File processed successfully!")
        
        tab1, tab2 = st.tabs(["Event Definitions", "Event Instruments"])
        
        with tab1:
            st.write(f"**Found {len(df_events)} event definitions**")
            if not df_events.empty:
                st.dataframe(df_events, use_container_width=True)
            else:
                st.info("No event definitions found.")
        
        with tab2:
            st.write(f"**Found {len(df_instruments)} event instruments**")
            if not df_instruments.empty:
                st.dataframe(df_instruments, use_container_width=True)
            else:
                st.info("No event instruments found.")
        
        output_filename = uploaded_file.name.replace('.xml', '_events.xlsx')
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if not df_events.empty:
                df_events.to_excel(writer, sheet_name='Event Definitions', index=False)
            if not df_instruments.empty:
                df_instruments.to_excel(writer, sheet_name='Event Instruments', index=False)
        output.seek(0)
        
        st.download_button(
            label="📥 Download Excel File",
            data=output,
            file_name=output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

with st.expander("ℹ️ How to use"):
    st.markdown("""
    1. **Upload** your ODM XML file
    2. **Preview** the extracted data
    3. **Download** the Excel file
    """)

st.markdown("---")
st.markdown("*ODM File Processor*")