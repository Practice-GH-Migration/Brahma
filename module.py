def validate_user(gh_client, username):
    """Validate that a user exists on GitHub"""
    try:
        user = gh_client.get_user(username.strip())
        return user.login if user else None
    except UnknownObjectException:
        log_and_print(f"User '{username}' not found on GitHub", "warning")
        return None
    except Exception as e:
        log_and_print(f"Error validating user '{username}': {str(e)}", "error")
        return None

def user_exists_in_org(gh_client, org_name, username):
    """Check if a user exists in the organization"""
    if not username or not username.strip():
        return False
        
    # First validate the user exists on GitHub
    validated_username = validate_user(gh_client, username)
    if not validated_username:
        return False
    
    rate_limited = True
    max_attempts = 2  # Prevent deep recursion
    attempt = 0
    
    while rate_limited and attempt < max_attempts:
        attempt += 1
        try:
            org = gh_client.get_organization(org_name)
            try:
                # Check if user is in the members list
                members = org.get_members()
                result = any(member.login == validated_username for member in members)
                rate_limited = False  # Success, exit the loop
                return result
            except GithubException as e:
                log_and_print(f"Error checking members for '{validated_username}': {str(e)}", "error")
                return False
        except RateLimitExceededException:
            log_and_print(f"Rate limit exceeded when checking if '{username}' is in org, waiting for reset", "warning")
            smart_rate_limit_handler(gh_client)
            # Will retry in the next loop iteration
        except Exception as e:
            log_and_print(f"Error checking user existence for '{username}': {str(e)}", "error")
            return False
    
    return False  # Return false if we exhausted our attempts

def create_gh_team(gh_target, org_name, team_name, description="", privacy="closed", target_teams_cache=None):
    """Create a new team or get existing team in target organization
    Args:
        gh_target: GitHub instance for target organization
        org_name: Name of target organization
        team_name: Name of team to create/update
        description: Team description
        privacy: Team privacy setting ('secret', 'closed', or 'visible')
        target_teams_cache: Cache of target teams to avoid repeated API calls
    """
    try:
        org = gh_target.get_organization(org_name)
        
        # Check if team exists using the cache
        # If cache is not provided, initialize an empty dict to avoid fallback behavior
        cache = target_teams_cache if target_teams_cache is not None else {}
        
        # Use the cache to check for existing team
        team = cache.get(team_name.lower())
        if team:
            log_and_print(f"Team '{team_name}' already exists in organization '{org_name}' with privacy '{team.privacy}'", "warning")
            return team, "exist"
            
        # Create a new team normally with retry capability
        try:
            team = org.create_team(
                name=team_name,
                description=description,
                privacy=privacy,
                notification_setting='notifications_enabled'
            )
            log_and_print(f"Created team '{team_name}' in organization '{org_name}' with privacy '{privacy}'", "success")
            return team, "new"
        except GithubException as e:
            # Handle specific GitHub exceptions
            if e.status == 422:
                log_and_print(f"Validation failed while creating team '{team_name}': {str(e)}", "error")
            elif e.status == 403:
                log_and_print(f"Permission denied while creating team '{team_name}': {str(e)}", "error")
            else:
                log_and_print(f"GitHub error creating team '{team_name}': {str(e)}", "error")
            raise  # Re-raise to trigger retry
            
    except RateLimitExceededException as e:
        log_and_print(f"Rate limit exceeded while creating team '{team_name}': {str(e)}", "error")
        raise  # Re-raise to trigger retry via decorator
    except Exception as e:
        log_and_print(f"Error creating/updating team '{team_name}' in '{org_name}': {str(e)}", "error")
        raise  # Re-raise to trigger retry via decorator

def set_team_parent(gh_target, org_name, team, parent_team):
    """Set parent team for a team
    Args:
        gh_target: GitHub instance for target organization
        org_name: Name of target organization
        team: Team to set parent for
        parent_team: Parent team
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Debug information
        log_and_print(f"Setting parent team - Team: {team.name} (ID: {team.id}), Parent: {parent_team.name} (ID: {parent_team.id})")
        
        # Use the PyGithub library's edit method to set the parent
        team.edit(name=team.name, parent_team_id=parent_team.id)
        log_and_print(f"Successfully set parent team '{parent_team.name}' for team '{team.name}'", "success")
        return True
            
    except Exception as e:
        log_and_print(f"Error setting parent team: {str(e)}", "error")
        return False

def get_team_by_name(gh_target, org_name, team_name):
    """Get team by name from organization
    Args:
        gh_target: GitHub instance for target organization
        org_name: Name of target organization
        team_name: Name of team to find
    Returns:
        Team object if found, None otherwise
    """
    try:
        org = gh_target.get_organization(org_name)
        return next((team for team in org.get_teams() if team.name == team_name), None)
    except Exception as e:
        log_and_print(f"Error getting team '{team_name}': {str(e)}", "error")
        return None

def migrate_team_members(source_team, target_team, gh_target, target_org):
    """Migrate members from source team to target team"""
    users_status = {}
    member_details = []
    members_migrated = 0
    members_failed = 0
    
    try:
        # Get source team members with rate limit handling
        source_members = []
        try:
            source_members = list(source_team.get_members())
        except RateLimitExceededException:
            log_and_print(f"Rate limit exceeded when getting members for team '{source_team.name}'. Waiting for reset...", "warning")
            # Try again after waiting
            source_members = list(source_team.get_members())
            
        log_and_print(f"Found {len(source_members)} members in source team '{source_team.name}'")
        
        for index, member in enumerate(source_members, 1):
            log_and_print(f"Processing user '{member.login}' for team '{source_team.name}' ({index}/{len(source_members)})")
            
            try:
                # Check if user exists in target organization
                if not user_exists_in_org(gh_target, target_org, member.login):
                    users_status[member.login] = {
                        'status': 'skipped',
                        'reason': 'User not found in target organization'
                    }
                    members_failed += 1
                    member_details.append(f"{member.login}: Skipped - User not found in target organization")
                    continue
                
                # Get user's role in source team with rate limit handling
                role = None
                try:
                    log_and_print(f"Checking current role for user '{member.login}' in team '{source_team.name}'")
                    role = 'maintainer' if source_team.get_team_membership(member).role == 'maintainer' else 'member'
                except RateLimitExceededException:
                    log_and_print(f"Rate limit exceeded when checking role for '{member.login}'. Waiting for reset...", "warning")
                    # Try again after waiting
                    role = 'maintainer' if source_team.get_team_membership(member).role == 'maintainer' else 'member'
                
                # Add user to target team with appropriate role
                log_and_print(f"Adding/Updating user '{member.login}' with role '{role}' to team '{source_team.name}'")
                try:
                    target_team.add_membership(member, role=role)
                except RateLimitExceededException:
                    log_and_print(f"Rate limit exceeded when adding '{member.login}'. Waiting for reset...", "warning")
                    smart_rate_limit_handler(target_team._requester._Requester__connection.rate_limiting)
                    # Try again after waiting
                    target_team.add_membership(member, role=role)
                
                users_status[member.login] = {
                    'status': 'success',
                    'role': role
                }
                members_migrated += 1
                member_details.append(f"{member.login}: Success - Added as {role}")
                log_and_print(f"Successfully added {member.login} as {role} to team '{source_team.name}'", "success")
                
            except RateLimitExceededException as e:
                log_and_print(f"Rate limit exceeded when processing '{member.login}'. This user will be skipped.", "error")
                users_status[member.login] = {
                    'status': 'failed',
                    'error': f"Rate limit exceeded: {str(e)}"
                }
                members_failed += 1
                member_details.append(f"{member.login}: Failed - Rate limit exceeded")
            except Exception as e:
                users_status[member.login] = {
                    'status': 'failed',
                    'error': str(e)
                }
                members_failed += 1
                member_details.append(f"{member.login}: Failed - {str(e)}")
                log_and_print(f"Error adding user '{member.login}' to team '{source_team.name}': {str(e)}", "error")
        
        return {
            'success': members_failed == 0,
            'total_members': len(source_members),
            'members_migrated': members_migrated,
            'members_failed': members_failed,
            'member_details': member_details
        }
            
    except RateLimitExceededException as e:
        error_msg = f"Rate limit exceeded during team member migration: {str(e)}"
        log_and_print(error_msg, "error")
        return {
            'success': False,
            'total_members': 0,
            'members_migrated': members_migrated,
            'members_failed': members_failed,
            'member_details': member_details + [f"Error: Rate limit exceeded"]
        }
    except Exception as e:
        error_msg = f"Error migrating members for team '{source_team.name}': {str(e)} \nError type: {type(e)} \nError details: {str(e)}"
        log_and_print(error_msg, "error")
        return {
            'success': False,
            'total_members': 0,
            'members_migrated': members_migrated,
            'members_failed': members_failed,
            'member_details': member_details + [f"Error: {str(e)}"]
        }

def check_external_idp_group_exists(org_name, idp_group_name, token, display_name=None):
    """Check if an external IDP group with the given name exists
    Args:
        org_name: Name of the organization
        idp_group_name: Name of the IDP group to check
        token: GitHub token
        display_name: Optional display name to check (defaults to idp_group_name)
    Returns:
        bool: True if the group exists, False otherwise
    """
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    # If display_name is not provided, use the idp_group_name
    search_name = display_name or idp_group_name
    
    # Build URL to search for external groups
    url = f"{GH_TARGET_BASE_API_URL}/orgs/{org_name}/external-groups?display_name={search_name}"
    
    max_retries = 3
    retry_count = 0
    retry_delay = 5  # Initial delay in seconds
    
    while retry_count <= max_retries:
        try:
            log_and_print(f"Checking if external IDP group '{search_name}' exists in organization '{org_name}'")
            response = make_github_request('get', url, headers=headers)
            
            if (response.status_code == 200) or (response.status_code == 201):
                groups = response.json().get('groups', [])
                
                # Check if any group matches the search name
                matching_groups = [
                    group for group in groups 
                    if group['group_name'].lower() == search_name.lower() or 
                       search_name.lower() in group['group_name'].lower()
                ]
                
                if matching_groups:
                    group_name = matching_groups[0]['group_name']
                    log_and_print(f"Found matching external IDP group: '{group_name}'", "success")
                    return True
                else:
                    log_and_print(f"No matching external IDP group found for '{search_name}'", "warning")
                    return False
            elif response.status_code == 403 and "rate limit exceeded" in response.text.lower():
                retry_count += 1
                
                # Try to get reset time from headers
                reset_timestamp = None
                if 'x-ratelimit-reset' in response.headers:
                    reset_timestamp = int(response.headers['x-ratelimit-reset'])
                    wait_time = max(1, reset_timestamp - int(time.time()) + 1)
                else:
                    # Extract timestamp from error response if possible
                    match = re.search(r'timestamp (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', response.text)
                    if match:
                        try:
                            reset_time = datetime.datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S')
                            wait_time = max(1, int((reset_time - datetime.datetime.utcnow()).total_seconds()) + 1)
                        except:
                            # Use exponential backoff if timestamp parsing fails
                            wait_time = retry_delay * (2 ** (retry_count - 1))
                    else:
                        # Use exponential backoff
                        wait_time = retry_delay * (2 ** (retry_count - 1))
                
                if retry_count <= max_retries:
                    log_and_print(f"Rate limit exceeded. Attempt {retry_count}/{max_retries}. Waiting {wait_time} seconds before retry...", "warning")
                    time.sleep(wait_time)
                    continue
                else:
                    log_and_print(f"Maximum retries reached. Rate limit still exceeded.", "error")
                    return False
            else:
                log_and_print(f"Failed to check external IDP groups: {response.status_code} - {response.text}", "error")
                return False
                
        except Exception as e:
            log_and_print(f"Error checking external IDP group: {str(e)}", "error")
            
            # Only retry on connection errors, not other exceptions
            if isinstance(e, requests.exceptions.RequestException) and retry_count < max_retries:
                retry_count += 1
                wait_time = retry_delay * (2 ** (retry_count - 1))
                log_and_print(f"Connection error. Attempt {retry_count}/{max_retries}. Retrying in {wait_time} seconds...", "warning")
                time.sleep(wait_time)
            else:
                return False
    
    return False

def setup_logging(output_folder):
    """Setup logging"""
    # Create the output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    current_datetime = datetime.datetime.now().strftime('%d%b%Y_%H%M')
    log_file_path = os.path.join(output_folder, f"TEAM_MIGRATION_LOG_{current_datetime}.log")
    
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def remove_users_from_team(org_name, team_slug, token, users):
    """Remove users from a team
    
    Args:
        org_name: Name of the organization
        team_slug: Slug of the team
        token: GitHub token
        users: List of usernames to remove
    
    Returns:
        tuple: (success_count, failed_count, details)
    """
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    success_count = 0
    failed_count = 0
    details = []
    
    # Break users into batches to avoid overwhelming API
    batch_size = 10
    user_batches = [users[i:i + batch_size] for i in range(0, len(users), batch_size)]
    
    for batch in user_batches:
        for username in batch:
            username = username.strip()
            if not username:
                continue
                
            remove_url = f"{GH_TARGET_BASE_API_URL}/orgs/{org_name}/teams/{team_slug}/memberships/{username}"
            
            try:
                log_and_print(f"Removing user '{username}' from team '{team_slug}'")
                response = make_github_request('delete', remove_url, headers=headers)
                
                if response.status_code in [204, 404]:  # 204: Success, 404: User not in team
                    success_count += 1
                    details.append(f"{username}: Success - Removed from team")
                    log_and_print(f"Successfully removed user '{username}' from team '{team_slug}'", "success")
                elif response.status_code == 403:
                    # Permission denied - log but don't retry
                    failed_count += 1
                    details.append(f"{username}: Failed - Permission denied - {response.text}")
                    log_and_print(f"Permission denied removing user '{username}': {response.text}", "error")
                elif response.status_code >= 500:
                    # Server error - raise to trigger retry
                    details.append(f"{username}: Failed (will retry) - {response.status_code} - {response.text}")
                    log_and_print(f"Server error removing user '{username}' (will retry): {response.status_code}", "warning")
                    response.raise_for_status()
                else:
                    failed_count += 1
                    details.append(f"{username}: Failed - {response.status_code} - {response.text}")
                    log_and_print(f"Failed to remove user '{username}': {response.status_code} - {response.text}", "error")
            except requests.exceptions.RequestException as e:
                # For network or GitHub API exceptions, allow retry mechanism to work
                log_and_print(f"Network error removing user '{username}' (will retry): {str(e)}", "warning")
                raise
            except Exception as e:
                failed_count += 1
                details.append(f"{username}: Failed - {str(e)}")
                log_and_print(f"Error removing user '{username}': {str(e)}", "error")
        
        # Small delay between batches to avoid rate limiting
        if len(user_batches) > 1 and batch != user_batches[-1]:
            time.sleep(1)
    
    return success_count, failed_count, details

def map_external_group_to_team(org_name, team_slug, token, group_id=None, idp_group_cache=None):
    """Map an external group to a GitHub team
    Args:
        org_name: Name of the organization
        team_slug: Slug of the team
        token: GitHub token
        group_id: External group ID (optional, if None will look up by team name)
        idp_group_cache: Optional cache of IDP groups to avoid repeated API calls
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        # First check if the team is already mapped to the expected group
        is_mapped, existing_mapping = check_team_external_group_mapping(org_name, team_slug, token, team_slug)
        if is_mapped:
            log_and_print(f"Team '{team_slug}' is already mapped to external group. No changes needed.")
            return True
        
        # Use cache if provided, otherwise make API call
        if group_id is None:
            if idp_group_cache and team_slug.lower() in idp_group_cache:
                group_data = idp_group_cache[team_slug.lower()]
                group_id = group_data.get('group_id')
                group_name = group_data.get('group_name')
                log_and_print(f"Using cached external group '{group_name}' with ID '{group_id}' for team '{team_slug}'")
            else:
                log_and_print(f"Looking up external group ID for team '{team_slug}'")
                external_groups_url = f"{GH_TARGET_BASE_API_URL}/orgs/{org_name}/external-groups?display_name={team_slug}"
                response = make_github_request('get', external_groups_url, headers=headers)
                
                if response.status_code != 200:
                    log_and_print(f"Failed to get external groups by display name: {response.status_code} - {response.text}", "error")
                    return False
                    
                groups = response.json().get('groups', [])
                if not groups:
                    log_and_print(f"No external group found matching team name '{team_slug}'", "warning")
                    return False
                    
                # Find the best matching group by name
                matching_group = next((g for g in groups if g['group_name'].lower() == team_slug.lower()), None)
                
                if not matching_group:
                    log_and_print(f"No matching external group found for team '{team_slug}'", "warning")
                    return False
                    
                group_id = matching_group['group_id']
                group_name = matching_group['group_name']
                log_and_print(f"Found matching external group '{group_name}' with ID '{group_id}'", "success")
        
        # Map team to external group
        url = f"{GH_TARGET_BASE_API_URL}/orgs/{org_name}/teams/{team_slug}/external-groups"
        data = {
            'group_id': group_id
        }
        
        log_and_print(f"Mapping team '{team_slug}' to external group with ID '{group_id}'")
        response = make_github_request('patch', url, headers=headers, json_data=data)
        
        if response.status_code in [200, 201]:
            log_and_print(f"Successfully mapped team '{team_slug}' to external group", "success")
            return True
        elif response.status_code == 404:
            log_and_print(f"Team '{team_slug}' or external group not found: {response.text}", "error")
            return False
        elif response.status_code == 422:
            log_and_print(f"Validation failed for team '{team_slug}' mapping: {response.text}", "error")
            return False
        elif response.status_code == 403:
            log_and_print(f"Permission denied for mapping team '{team_slug}' to external group: {response.text}", "error")
            return False
        else:
            log_and_print(f"Failed to map team to external group: {response.status_code} - {response.text}", "error")
            # For server errors (500s) or network issues, raise the exception to trigger retry
            response.raise_for_status()
            return False
    except requests.exceptions.RequestException as e:
        log_and_print(f"Network error mapping team to external group: {str(e)}", "error")
        raise  # Raise to trigger retry
    except Exception as e:
        log_and_print(f"Error mapping team to external group: {str(e)}", "error")
        raise  # Raise to trigger retry

def check_team_external_group_mapping(org_name, team_slug, token, expected_group_name=None):
    """Check if a team is already mapped to an external group
    
    Args:
        org_name: Name of the organization
        team_slug: Slug of the team
        token: GitHub token
        expected_group_name: Name of the external group to check for (optional)
        
    Returns:
        tuple: (is_mapped, group_data) where:
            - is_mapped is a boolean indicating if the team is mapped to the expected group
            - group_data is a dict with details about the current mapping (or None if no mapping)
    """
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    url = f"{GH_TARGET_BASE_API_URL}/orgs/{org_name}/teams/{team_slug}/external-groups"
    
    try:
        log_and_print(f"Checking if team '{team_slug}' is already mapped to an external group")
        response = make_github_request('get', url, headers=headers)
        
        if response.status_code == 200:
            mapping_data = response.json()
            
            # Check if there's an existing mapping
            if mapping_data and 'groups' in mapping_data and mapping_data['groups']:
                group_data = mapping_data['groups'][0]  # Get the first (and typically only) mapping
                group_name = group_data.get('group_name')
                group_id = group_data.get('group_id')

                log_and_print(f"Team '{team_slug}' is already correctly mapped to external group.  No changes needed.", "warning")
                # If expected_group_name is provided, check if it matches
                if expected_group_name:
                    is_expected_group = (group_name.lower() == expected_group_name.lower() or 
                                        expected_group_name.lower() in group_name.lower())
                    
                    if is_expected_group:
                        log_and_print(f"Team '{team_slug}' is corectly mapped to the expected group '{group_name}'","success")
                        return True, group_data
                    else:
                        log_and_print(f"Team '{team_slug}' is mapped to group '{group_name}' but expected '{expected_group_name}'")
                        return False, group_data
                else:
                    # If no expected name provided, just return that it's mapped
                    return True, group_data
            else:
                log_and_print(f"Team '{team_slug}' is not mapped to any external group")
                return False, None
        elif response.status_code == 404:
            log_and_print(f"Team '{team_slug}' not found or has no external group mapping")
            return False, None
        else:
            log_and_print(f"Error checking team external group mapping: {response.status_code} - {response.text}", "error")
            return False, None
    except Exception as e:
        log_and_print(f"Error checking team external group mapping: {str(e)}", "error")
        return False, None


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# function to use smart rate handling and implement its core functionality
def migrate_teams_optimized(gh_source, gh_target, source_org, target_org, output_folder, teams_to_migrate=None, migrate_users=False, map_idp_groups=False, migrate_parent_child=False):
    """Optimized version of team migration that minimizes API calls"""
    
    # Cache for API responses
    team_cache = {}
    migration_status = {}
    migration_details = {}
    
    # Get source org object
    source_org_obj = gh_source.get_organization(source_org)
    
    # Check if specific teams are requested for migration
    if teams_to_migrate:
        log_and_print(f"Fetching specific teams ({len(teams_to_migrate)}) from source organization...")
        for team_name in teams_to_migrate:
            try:
                team = source_org_obj.get_team_by_slug(team_name.strip())
                team_cache[team.slug] = {
                    'name': team.name,
                    'description': team.description or '',
                    'privacy': team.privacy,
                    'parent': team.parent.slug if team.parent else None,
                    'team_obj': team  # Store the team object to avoid re-fetching
                }
                log_and_print(f"Fetched team: {team_name}")
            except RateLimitExceededException:
                log_and_print(f"Rate limit reached fetching team '{team_name}'. Waiting for reset...", "error")
                smart_rate_limit_handler(gh_source)
                # Try again after waiting
                team = source_org_obj.get_team_by_slug(team_name.strip())
                team_cache[team.slug] = {
                    'name': team.name,
                    'description': team.description or '',
                    'privacy': team.privacy,
                    'parent': team.parent.slug if team.parent else None,
                    'team_obj': team
                }
            except Exception as e:
                log_and_print(f"Error fetching team '{team_name}': {str(e)}", "error")
                # Continue with other teams
        
        log_and_print(f"Successfully fetched {len(team_cache)} teams from source organization")
    else:
        # Pre-fetch all teams from source in one API call if no specific teams requested
        log_and_print(f"Pre-fetching all teams from source organization...")
        
        try:
            # Get all source teams
            all_source_teams = list(source_org_obj.get_teams())
            log_and_print(f"Found {len(all_source_teams)} teams in source organization")
            
            # Cache teams from source organization
            for team in all_source_teams:
                team_cache[team.slug] = {
                    'name': team.name,
                    'description': team.description or '',
                    'privacy': team.privacy,
                    'parent': team.parent.slug if team.parent else None,
                    'team_obj': team  # Store the team object to avoid re-fetching
                }
        except RateLimitExceededException:
            log_and_print("Rate limit reached during initial team fetch. Waiting for reset...", "error")
            smart_rate_limit_handler(gh_source)
            # Retry
            return migrate_teams_optimized(gh_source, gh_target, source_org, target_org, output_folder, 
                                          teams_to_migrate, migrate_users, map_idp_groups, migrate_parent_child)
    
    # Determine teams to migrate (now based on what we have in the cache)
    if not teams_to_migrate:
        teams_to_migrate = list(team_cache.keys())
    
    # PRE-FETCH TARGET TEAMS - critical optimization
    log_and_print(f"Pre-fetching all teams from target organization...")
    target_org_obj = gh_target.get_organization(target_org)
    target_teams_cache = {}
    
    try:
        all_target_teams = list(target_org_obj.get_teams())
        log_and_print(f"Found {len(all_target_teams)} teams in target organization")
        
        # Cache teams by name AND slug (case insensitive)
        for team in all_target_teams:
            target_teams_cache[team.name.lower()] = team  # Index by name
            target_teams_cache[team.slug.lower()] = team  # Also index by slug
    except RateLimitExceededException:
        log_and_print("Rate limit reached during target team fetch. Waiting for reset...", "error")
        smart_rate_limit_handler(gh_target)
        # Retry
        return migrate_teams_optimized(gh_source, gh_target, source_org, target_org, output_folder,     
                                      teams_to_migrate, migrate_users, map_idp_groups, migrate_parent_child)
    
    # PRE-FETCH IDP GROUPS if mapping enabled
    idp_group_cache = {}
    if map_idp_groups:
        try:
            log_and_print("Pre-fetching all available IDP groups...")
            idp_groups = get_all_external_groups(target_org, GH_TARGET_TOKEN)
            
            # Cache IDP groups by name (case insensitive)
            for group in idp_groups:
                group_name = group['group_name'].lower()
                idp_group_cache[group_name] = group
                
                # Also cache by aliases or similar names if needed
                # Example: "Team-XYZ" might be "team_xyz" or "team xyz" in IDP
                normalized_name = group_name.replace('-', '_').replace(' ', '_')
                if normalized_name != group_name:
                    idp_group_cache[normalized_name] = group
            
            log_and_print(f"Cached {len(idp_group_cache)} external IDP groups")
        except Exception as e:
            log_and_print(f"Error fetching IDP groups: {str(e)}", "error")
    
    # Track parent-child relationships
    parent_child_relationships = []
    if migrate_parent_child:
        for team_slug, team_info in team_cache.items():
            if team_info.get('parent'):
                parent_child_relationships.append((team_slug, team_info['parent']))
                
        # Optimize creation order for parent-child relationships
        teams_to_migrate = optimize_parent_child_migration(parent_child_relationships, team_cache)
        log_and_print(f"Optimized team creation order for parent-child relationships")
    
    # Process teams in optimized order with batch processing
    total_teams = len(teams_to_migrate)
    log_and_print(f"Starting migration of {total_teams} teams")
    
    # Process in batches to avoid hitting rate limits too frequently
    batch_size = 100  # Adjust as needed
    batches = [teams_to_migrate[i:i+batch_size] for i in range(0, len(teams_to_migrate), batch_size)]
    
    for batch_num, batch in enumerate(batches, 1):
        log_and_print(f"Processing batch {batch_num}/{len(batches)} ({len(batch)} teams)")
        
        for index, team_name in enumerate(batch, 1):
            log_and_print(f"Processing team {(batch_num-1)*batch_size + index}/{total_teams}: {team_name}")
            
            # Get team from cache
            team_info = team_cache.get(team_name.lower())
            if not team_info:
                log_and_print(f"Team {team_name} not found in source cache, skipping", "error")
                migration_status[team_name] = "failed"
                continue
            
            try:
                # Initialize team details
                team_details = {
                    'description': team_info['description'],
                    'privacy': team_info['privacy'],
                    'total_members': 0,
                    'members_migrated': 0,
                    'members_failed': 0,
                    'member_details': [],
                    'idp_group_name': team_name,
                    'idp_mapping_status': 'not_attempted'
                }
                
                # Check if IDP group exists using cache before proceeding
                idp_group_exists = False
                if map_idp_groups:
                    idp_group_exists = team_name.lower() in idp_group_cache
                    
                    # if not idp_group_exists:
                    #     # Try alternate spellings
                    #     alt_name = team_name.lower().replace('-', '_').replace(' ', '_')
                    #     if alt_name in idp_group_cache:
                    #         idp_group_exists = True
                    #         log_and_print(f"Found IDP group using alternate spelling: {alt_name}", "warning")
                    
                    if not idp_group_exists:
                        log_and_print(f"IDP group '{team_name}' not found in cache. Skipping team creation.", "warning")
                        migration_status[team_name] = "skipped"
                        team_details['idp_mapping_status'] = 'group_not_found'
                        migration_details[team_name] = team_details
                        continue
                
                # Get source team object from cache
                source_team = team_info.get('team_obj')
                if not source_team:
                    # Fall back to API call if not in cache
                    source_team = source_org_obj.get_team_by_slug(team_name)
                
                # Create team in target org using cached teams
                target_team, new_or_exist = create_gh_team(
                    gh_target,
                    target_org,
                    team_name,
                    team_info['description'],
                    team_info['privacy'],
                    target_teams_cache  # Pass cached teams to avoid repeated listing
                )
                
                if target_team:
                    # Add new team to cache
                    if new_or_exist == "new":
                        target_teams_cache[team_name.lower()] = target_team
                        target_teams_cache[target_team.slug.lower()] = target_team
                    
                    # Handle IDP mapping if requested
                    if map_idp_groups and idp_group_exists:
                        try:
                            # Only check rate limit before a potentially expensive operation
                            if index % 10 == 0:  # Check every 10th team
                                smart_rate_limit_handler(gh_target)
                                
                            # Only need to fetch members and remove if new team or not already mapped
                            is_mapped, _ = check_team_external_group_mapping(target_org, team_name, GH_TARGET_TOKEN, team_name)
                            
                            if not is_mapped:
                                # Remove existing members only if team is not already mapped
                                target_team_members = []
                                try:
                                    log_and_print(f"Getting existing members of team '{team_name}' before IDP mapping")
                                    for member in target_team.get_members():
                                        target_team_members.append(member.login)
                                except RateLimitExceededException:
                                    log_and_print(f"Rate limit exceeded when getting members. Waiting for reset...", "warning")
                                    smart_rate_limit_handler(gh_target)
                                    # Try again after waiting
                                    for member in target_team.get_members():
                                        target_team_members.append(member.login)
                                
                                if target_team_members:
                                    log_and_print(f"Removing {len(target_team_members)} existing members from team '{team_name}' before IDP mapping")
                                    success_count, failed_count, details = remove_users_from_team(
                                        target_org, team_name, GH_TARGET_TOKEN, target_team_members
                                    )
                                    log_and_print(f"Removed {success_count} members, {failed_count} failed")
                            
                                # Use the cached group_id if available
                                group_id = None
                                group_data = idp_group_cache.get(team_name.lower())
                                if group_data:
                                    group_id = group_data.get('group_id')
                                
                                # Map the team to the IDP group using the cache
                                if map_external_group_to_team(target_org, team_name, GH_TARGET_TOKEN, group_id, idp_group_cache):
                                    log_and_print(f"Successfully mapped team '{team_name}' to IDP group", "success")
                                    team_details['idp_mapping_status'] = 'success'
                                else:
                                    log_and_print(f"Failed to map team '{team_name}' to IDP group", "error")
                                    team_details['idp_mapping_status'] = 'failed'
                            else:
                                log_and_print(f"Team '{team_name}' is already mapped to an IDP group. Skipping mapping.", "success")
                                team_details['idp_mapping_status'] = 'already_mapped'
                                
                        except Exception as e:
                            log_and_print(f"Error mapping team to IDP group: {str(e)}", "error")
                            team_details['idp_mapping_status'] = 'failed'
                    
                    # Migrate users if needed and not using IDP mapping
                    if migrate_users and not map_idp_groups:
                        # Only check rate limit before a potentially expensive operation
                        if index % 5 == 0:  # Check every 5th team for user migration
                            smart_rate_limit_handler(gh_target)
                        
                        # Migrate members with optimized approach
                        member_results = migrate_team_members(source_team, target_team, gh_target, target_org)
                        migration_status[team_name] = "success" if member_results['success'] else "partial_success"
                        
                        # Update details with member results
                        team_details.update({
                            'total_members': member_results['total_members'],
                            'members_migrated': member_results['members_migrated'],
                            'members_failed': member_results['members_failed'],
                            'member_details': member_results['member_details']
                        })
                    else:
                        migration_status[team_name] = "success"
                        
                elif new_or_exist == "exist":
                    log_and_print(f"Team '{team_name}' already exists in the target organization.", "info")
                    migration_status[team_name] = "exists"
                    if map_external_group_to_team(target_org, team_name, GH_TARGET_TOKEN):
                        log_and_print(f"Successfully mapped team '{team_name}' to IDP group", "success")
                        team_details['idp_mapping_status'] = 'success'
                    else:
                        log_and_print(f"Failed to map team '{team_name}' to IDP group", "error")
                        team_details['idp_mapping_status'] = 'failed'
                else:
                    migration_status[team_name] = "failed"
                
                migration_details[team_name] = team_details
                
            except RateLimitExceededException:
                log_and_print(f"Rate limit reached processing team '{team_name}'. Waiting for reset...", "error")
                smart_rate_limit_handler(gh_target)
                # Don't update status - retry this team in a recursive call
                
                # Create a new list with remaining teams
                remaining_teams = batch[batch.index(team_name):] + [t for b in batches[batch_num:] for t in b]
                return migrate_teams_optimized(gh_source, gh_target, source_org, target_org, output_folder,
                                            remaining_teams, migrate_users, map_idp_groups, migrate_parent_child)
                                            
            except Exception as e:
                log_and_print(f"Error processing team '{team_name}': {str(e)}", "error")
                migration_status[team_name] = "failed"
                migration_details[team_name] = team_details
        
        # After each batch, check rate limit status
        if batch_num < len(batches):
            smart_rate_limit_handler(gh_target)
    
    # Final step: Handle parent-child relationships if needed
    if migrate_parent_child and parent_child_relationships:
        log_and_print("Setting up parent-child relationships")
        
        # Process parent-child in batches to avoid rate limiting
        for batch_num, relationship_batch in enumerate(
            [parent_child_relationships[i:i+50] for i in range(0, len(parent_child_relationships), 50)], 1
        ):
            log_and_print(f"Processing parent-child batch {batch_num}/{(len(parent_child_relationships) + 49) // 50}")
            
            for child, parent in relationship_batch:
                if child in migration_status and migration_status[child] in ["success", "exists"]:
                    try:
                        # Use cached objects when possible
                        target_team = target_teams_cache.get(child.lower())
                        parent_team = target_teams_cache.get(parent.lower())
                        
                        # Fall back to API call if not in cache
                        if not target_team:
                            target_team = get_team_by_name(gh_target, target_org, child)
                            if target_team:
                                target_teams_cache[child.lower()] = target_team
                                
                        if not parent_team:
                            parent_team = get_team_by_name(gh_target, target_org, parent)
                            if parent_team:
                                target_teams_cache[parent.lower()] = parent_team
                        
                        if target_team and parent_team:
                            set_team_parent(gh_target, target_org, target_team, parent_team)
                    except Exception as e:
                        log_and_print(f"Error setting parent for team '{child}': {str(e)}", "error")
            
            # Check rate limit after each batch
            if batch_num < (len(parent_child_relationships) + 49) // 50:
                smart_rate_limit_handler(gh_target)
    
    # Write final summary
    successful_migrations = [team for team, status in migration_status.items() if status in ["success", "exists"]]
    failed_migrations = [team for team, status in migration_status.items() if status not in ["success", "exists"]]
    
    write_migration_summary(
        output_folder,
        source_org,
        target_org,
        successful_migrations,
        failed_migrations,
        migration_details
    )
    return all(status in ["success", "exists"] for status in migration_status.values())
    
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
